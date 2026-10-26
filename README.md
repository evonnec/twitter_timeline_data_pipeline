# Twitter Timeline Data Pipeline

A serverless pipeline on AWS that copies a public X (Twitter) account's timeline into your own storage. A scheduled Lambda function reads new Tweets from the X API v2 and puts one SQS message on a queue for each Tweet. A second Lambda function consumes the queue, writes each Tweet's metadata to DynamoDB, and streams attached photos and videos into S3. The whole stack is one AWS SAM template, and the code is Python 3.14 managed with [uv](https://docs.astral.sh/uv/).

## Requirements

The project does the following:

- Authenticates to the X API v2 with a bearer token and collects a given user's timeline Tweets, each Tweet's metadata (`created_at`, `text`, `referenced_tweets`, and more), and any attached media.
- Uses Lambda, SQS, S3 and DynamoDB. A scheduled Lambda fetches and enqueues. A queue-triggered Lambda stores metadata in DynamoDB and media in S3.
- Is efficient, reliable and scalable: paginated fetches with media expanded inline, at-least-once delivery with repeatable writes, retries, a dead-letter queue, alarms, and bounded concurrency.
- Keeps the token in Secrets Manager.
- Is deployable from a YAML CloudFormation (SAM) template that creates the tables, bucket and queues, with a separate Python module for each Lambda function.
- Is documented, including the architecture, data models and assumptions (this file).

## Architecture

```mermaid
flowchart LR
    EB[EventBridge schedule] -->|invoke| F[FetchFunction]
    F -->|GET users and tweets| X[X API v2]
    F <-->|read token| SM[Secrets Manager<br/>bearer token]
    F <-->|checkpoint: newest Tweet ID| ST[(State table)]
    F -->|SendMessageBatch, one message per Tweet| Q[TweetsQueue]
    Q -->|batches of up to 10| P[ProcessFunction]
    Q -.->|after 5 failed receives| DLQ[TweetsDeadLetterQueue]
    P -->|PutItem| TT[(Tweets table)]
    P -->|stream upload| S3[(S3 media bucket)]
    P -->|download media| CDN[X media CDN]
    CW[CloudWatch alarms] -.->|watch| DLQ
    CW -.->|watch| F
    CW -.->|watch| P
```

A run, step by step:

1. EventBridge invokes `FetchFunction` on the schedule (`rate(1 hour)` by default). The event may carry `{"username": "..."}`; otherwise the `TWITTER_USERNAME` setting is used. The name is normalized once (leading `@` removed, lower-cased) and used everywhere below.
2. The function reads the bearer token from Secrets Manager (cached in memory for 15 minutes), resolves the username to a user ID, and reads the account's checkpoint (the newest Tweet ID already fetched) from the state table.
3. It pages through `GET /2/users/:id/tweets` with `since_id` set to the checkpoint, up to `MAX_PAGES` pages of `MAX_RESULTS` Tweets, newest first.
4. Each page becomes one message per Tweet, sent to the queue with `SendMessageBatch` (10 at a time; only entries SQS reports as failed are retried).
5. Only after every message of the run is enqueued does it advance the checkpoint.
6. SQS invokes `ProcessFunction` with batches of up to 10 messages (up to 10 seconds of batching, at most 5 concurrent invocations).
7. For each message it downloads the media and uploads it to S3, then writes the Tweet item to DynamoDB. Media goes first so a stored Tweet never points at missing media.
8. Messages that fail are reported back individually. SQS redelivers them and, after 5 receives, moves them to the dead-letter queue, which has an alarm.

## Data model

### SQS message

One message per Tweet. The body is compact, ASCII-escaped JSON (so odd characters in Tweet text cannot make SQS reject it) with `"schema_version": 1`; the processor rejects any other version. This example is generated from the real model, with fake IDs:

```json
{
  "username": "nasa",
  "tweet": {
    "id": "1850000000000000001",
    "text": "Launch day. https://t.co/abc",
    "note_text": null,
    "created_at": "2024-10-25T14:30:00.000Z",
    "author_id": "11348282",
    "conversation_id": "1850000000000000001",
    "lang": "en",
    "referenced_tweets": [],
    "public_metrics": {
      "retweet_count": 12,
      "reply_count": 3,
      "like_count": 150,
      "quote_count": 1
    },
    "edit_history_tweet_ids": ["1850000000000000001"],
    "media_keys": ["3_1850000000000000002"]
  },
  "media": [
    {
      "media_key": "3_1850000000000000002",
      "type": "photo",
      "url": "https://pbs.twimg.com/media/EXAMPLE.jpg",
      "preview_image_url": null,
      "variants": [],
      "width": 1200,
      "height": 800,
      "duration_ms": null,
      "alt_text": null
    }
  ],
  "fetched_at": "2024-10-25T14:35:00Z",
  "schema_version": 1
}
```

You can regenerate it with `TweetMessage(...).to_json()` from `twitter_timeline_data_pipeline.models`.

**Long posts.** For a post over 280 characters the API's `text` is only a truncated preview; the full body is in `note_tweet.text`, which the fetch Lambda requests. It travels as `tweet.note_text` (`null` for a short post). Entities of the long text are not collected.

**Compatibility.** `note_text` is an additive, optional field, so `schema_version` stays 1. A reader builds a Tweet from explicit keys and ignores unknown ones, so a processor from before this field still parses a new message (and stores the preview), and a new processor reading an old message gets `note_text = null`. The version only changes for a change a reader cannot ignore.

**Size guard.** The fetch Lambda checks each body against 256 KiB, the SQS default message size limit (queues can be configured for more; 256 KiB is the conservative choice). A body over the limit is logged at ERROR with the Tweet ID and size, skipped without stopping the run, and counted as `oversized` in the run summary. The largest possible post (25,000 weighted characters: 12,500 emoji or 12,500 CJK characters) is about 150 KB or 75 KB once ASCII-escaped, so this should only trigger on a malformed payload.

### Tweets table (`TweetsTable`)

| Item | Value |
| --- | --- |
| Partition key | `tweet_id` (S) |
| GSI `username-created_at-index` | partition `username` (S), sort `created_at` (S), projects `text`, `media_count` and `public_metrics` only |
| Billing | on demand; point-in-time recovery and encryption on |

`created_at` is stored as the API's ISO 8601 string, which sorts chronologically. The index projects `INCLUDE` rather than `ALL`: that roughly halves its write units and storage per Tweet. The trade-off is that a listing returns only those three attributes plus the keys, so read anything else (`media`, `referenced_tweets`, ...) with `GetItem` on `tweet_id`. Attributes written by `TweetStore.to_item` (attributes whose value is `None` are omitted):

| Attribute | Type | Notes |
| --- | --- | --- |
| `tweet_id` | S | Tweet ID |
| `username` | S | Normalized account name (lower case) |
| `author_id` | S | |
| `text` | S | The full text; for a long post, `note_tweet.text` |
| `display_text` | S | Only for a long post: the API's truncated preview |
| `is_long_post` | BOOL | `true` when the post has a `note_tweet`, else `false` |
| `created_at` | S | ISO 8601 |
| `conversation_id` | S | |
| `lang` | S | |
| `referenced_tweets` | L of M | `{type, id}`; type is `retweeted`, `quoted` or `replied_to` |
| `public_metrics` | M | retweet, reply, like and quote counts at fetch time |
| `edit_history_tweet_ids` | L of S | |
| `media` | L of M | one entry per media item (below) |
| `media_count` | N | |
| `fetched_at` | S | when the fetch Lambda saw the Tweet |
| `stored_at` | S | when the item was written |
| `schema_version` | N | |

Each `media` entry has `media_key`, `type`, `status` (`stored` and `exists` for files in S3, `unavailable` or `skipped`) and, when known, `s3_key`, `content_type`, `source_url` and `size_bytes`.

### State table (`StateTable`)

One item per account: partition key `username` (S, lower case), `newest_id` (N, the newest Tweet ID enqueued) and `updated_at` (S). The checkpoint only ever moves forward, through a conditional write.

### S3 media bucket

Key layout:

```text
media/<author_id>/<tweet_id>/<media_key><extension>
```

If a Tweet has no `author_id`, the lower-cased username is used instead. The key is deterministic, so a repeat delivery finds the object and skips the download. Photos are fetched at original size (`name=orig`, other query parameters are kept). For videos and animated GIFs, the highest-bit-rate `video/mp4` variant is stored.

Each object has the `Content-Type` of the source and user metadata `tweet-id`, `media-key` and `source-url`. The bucket is encrypted (SSE-S3), blocks all public access, refuses non-TLS requests, and aborts incomplete multipart uploads after 7 days. Versioning is on, so a deleted or overwritten object can be recovered for 30 days (noncurrent versions then expire). Objects under `media/` move to S3 Intelligent-Tiering on day 0, so cold video stops costing Standard rates; the trade-off is a small per-object monitoring fee, and objects under 128 KB are never tiered.

## Design decisions

- **`GET /2/users/:id/tweets`.** It returns an account's own Tweets and works with an app-only bearer token (the home timeline endpoint needs user-context OAuth). One request returns up to 100 Tweets with their media expanded, instead of one lookup per Tweet, which keeps API usage low.
- **At-least-once, with repeatable writes.** SQS can deliver a message twice, and a failed fetch run is re-run from the old checkpoint. Tweets are written with `PutItem` keyed by `tweet_id`, so a repeat overwrites the same item. Writes are last-write-wins, not conditional: a stale redelivery (for example a DLQ redrive of an old message) overwrites newer data such as `public_metrics`. Media keys are deterministic and existing objects are not downloaded again.
- **Checkpoint after enqueue.** If a run fails midway, the next run re-sends some Tweets (harmless duplicates) rather than losing any. Loss and delay cases are listed under limitations: the page-cap gap loses Tweets, and a batch that fails repeatedly (for example a media timeout) ends in the DLQ without its metadata being written.
- **Overlapping fetch runs are harmless.** The fetch function has no reserved concurrency. The checkpoint write is conditional, so it never moves backwards, and duplicate messages are handled by idempotent processing.
- **Partial batch responses, DLQ and alarms.** `ProcessFunction` returns the IDs of failed records, so one bad message does not retry the other nine. Transient media errors (timeouts, 429, 5xx) fail the record for retry. Malformed messages and unexpected errors fail it too and end up in the DLQ after 5 receives (kept 14 days). Alarms cover a non-empty DLQ, fetch errors (1 hour window) and whole-invocation process errors (5 minute window). Because of partial batch responses, a failed record is not a Lambda `Errors` datapoint, so the DLQ alarm is the signal for record-level failures. DLQ messages expire after 14 days. Set `AlarmTopicArn` to get notified through SNS.
- **Streaming uploads.** Media is downloaded only over https from `pbs.twimg.com` and `video.twimg.com` (message bodies are treated as untrusted), redirects are not followed, and the file is streamed from the CDN straight into S3 `upload_fileobj` through a small file-like adapter, so a large video is never held in memory.
- **Least-privilege IAM.** The fetch function can send to its queue, read one secret, and get/put items in the state table. The process function can write to the tweets table, put/get objects in the bucket and list it (so a missing key is a 404, not a 403). Neither can touch the other's resources.
- **Secrets handling.** The stack creates an empty secret, so the token is never in the template, a parameter or an environment variable. `scripts/put_secret.sh` fills it in without putting the token in the process arguments. The fetch function only gets the secret's ARN. It caches the token for 15 minutes, so a rotated secret is picked up without a redeploy, and drops the cache on a 401/403. The token is sent only in the `Authorization` header and is excluded from log output and `repr`. The AWS SDK and HTTP library loggers (`botocore`, `boto3`, `s3transfer`, `urllib3`, `httpcore`, `httpx`) are held at WARNING even when `LogLevel` is `DEBUG`, since at DEBUG they can log secret values.
- **Retained data stores.** The tweets table and the media bucket have `DeletionPolicy: Retain`, so deleting the stack does not delete collected data.
- **No automatic fetch retries.** The function has `MaximumRetryAttempts: 0`, because the next scheduled run is the retry and a retry would only use up API rate limit.
- **Runtime.** Python 3.14 on arm64 (Graviton). The dependency bundle is built for `aarch64-manylinux_2_28` wheels so it can be built on any machine.

## Repository layout

```text
.
├── template.yaml                      SAM/CloudFormation template for the whole stack
├── samconfig.toml                     sam deploy defaults (stack name, region, capabilities)
├── pyproject.toml, uv.lock            package metadata and locked dependencies
├── renovate.json                      dependency update configuration
├── LICENSE                            GPL-3.0
├── .github/workflows/ci.yml           CI, release and optional PyPI publish
├── .python-version                    3.14
├── events/
│   ├── fetch.json                     sample event for FetchFunction
│   └── sqs.json                       sample SQS event for ProcessFunction
├── scripts/
│   ├── build_lambda.sh                build build/lambda/ for Python 3.14 on arm64
│   ├── deploy.sh                      build, then sam deploy
│   ├── put_secret.sh                  store the bearer token in Secrets Manager
│   └── delete.sh                      delete the stack
├── src/twitter_timeline_data_pipeline/
│   ├── config.py                      settings read from environment variables
│   ├── credentials.py                 bearer token lookup and TTL cache
│   ├── x_api.py                       X API v2 client (retries, rate limits, pagination)
│   ├── models.py                      Tweet, Media, SQS message (schema_version 1)
│   ├── storage.py                     DynamoDB and S3 persistence, checkpoint store
│   ├── cli.py                         `twitter-pipeline` command
│   └── handlers/
│       ├── fetch.py                   FetchFunction handler
│       └── process.py                 ProcessFunction handler
└── tests/                             moto-mocked tests, no AWS account needed
```

## Local development

The project is pinned to Python 3.14 in `.python-version`. Install [uv](https://docs.astral.sh/uv/getting-started/installation/); it downloads Python 3.14 if you don't have it.

```bash
uv sync                      # create .venv with runtime and dev dependencies
uv run pytest                # tests
uv run ruff check .          # lint
uv run ruff format --check . # formatting
uv run mypy                  # strict type check of src/
uv build                     # build the sdist and wheel into dist/
```

## Continuous integration and releases

`.github/workflows/ci.yml` runs on every pull request, on pushes to `main` and on `v*` tags. Each job installs uv and uses the locked dependencies (`uv sync --locked`).

| Job | What it runs |
| --- | --- |
| Lint | `uv lock --check` (lockfile is current) and `ruff check` |
| Format | `ruff format --check` |
| Type check | `mypy` (strict) |
| Test | `pytest` with coverage on Python 3.14 and 3.15; 3.15 shows readiness for the next Lambda runtime. JUnit and coverage reports are uploaded as artifacts |
| Validate template | `cfn-lint template.yaml` and `shellcheck scripts/*.sh` |
| Build | After all of the above: `uv build`, a smoke test of the wheel in an isolated environment (import, `--help`, `--version`), and the Lambda bundle zipped as `twitter_timeline_data_pipeline-<version>-lambda-arm64.zip` |
| GitHub Release | On `v*` tags only: checks the tag, then creates the release |

### Cutting a release

Versions follow [semantic versioning](https://semver.org). The version lives in `pyproject.toml`, and the tag must match it.

```bash
uv version --bump patch      # or minor, or major
git commit -am "release v$(uv version --short)"
# open a PR, let CI pass, merge it, then from an up-to-date main:
git tag "v$(uv version --short)"
git push origin "v$(uv version --short)"
```

For a prerelease, add a second bump: `uv version --bump minor --bump rc` gives `0.2.0rc1`, and `uv version --bump rc` moves to `0.2.0rc2`. `uv version --bump stable` drops the suffix. uv writes prereleases in Python's spelling (`0.2.0rc1`) and the tag uses the semver spelling (`v0.2.0-rc.1`; `-alpha.N` and `-beta.N` work the same way). The release job maps one to the other.

The release job fails unless the tag is valid semver (`vMAJOR.MINOR.PATCH`, optionally `-prerelease`, no leading zeros) and equals the version in `pyproject.toml`. It then creates a GitHub Release with generated notes and attaches the wheel, the sdist and the Lambda zip. A tag with a prerelease suffix is marked as a prerelease.

### Publishing to PyPI (off by default)

This repository is private, and publishing to PyPI would make the package public, so GitHub Releases are the distribution channel. The `publish-pypi` job runs after the release job and only when the repository variable `PUBLISH_TO_PYPI` is `true`. To enable it:

1. On PyPI, add a trusted publisher for this repository with workflow `ci.yml` and environment `pypi`.
2. Create the `pypi` environment in the repository settings, ideally limited to `v*` tags with required reviewers.
3. Set the repository variable `PUBLISH_TO_PYPI` to `true`.

## Running locally

The `twitter-pipeline` command runs the fetch step from a terminal. A dry run needs only a bearer token and makes no AWS calls:

```bash
export TWITTER_BEARER_TOKEN='<your bearer token>'
uv run twitter-pipeline fetch --username NASA --dry-run
```

It resolves the username, pages through the timeline, and prints one SQS message body per Tweet to stdout, as compact JSON on one line each. Logs go to stderr. Nothing is sent to SQS and no checkpoint is read or written. Without `--dry-run` it does the real fetch (needs `QUEUE_URL`, `STATE_TABLE` and AWS credentials) and prints a JSON summary of the run.

Other options: `--max-pages`, `--max-results` (5 to 100), `--since-id` and `--log-level`; see `uv run twitter-pipeline fetch --help`. Exit codes: 0 success; 1 an upstream or runtime failure (X API, malformed API data, an unreadable secret, enqueue, or an AWS SDK error such as SQS or DynamoDB access denied; the one-line message goes to stderr and the traceback shows with `--log-level DEBUG`); 2 bad arguments or configuration (invalid username, out-of-range option, missing `QUEUE_URL` or `STATE_TABLE`, or no bearer token configured).

### Environment variables

| Variable | Used by | Default | Meaning |
| --- | --- | --- | --- |
| `TWITTER_USERNAME` | fetch | none | Account to fetch when the event has no `username` |
| `BEARER_TOKEN_SECRET_ARN` | fetch | none | Secrets Manager ARN or name of the token. The secret may be the raw token or JSON with a `bearer_token` key |
| `TWITTER_BEARER_TOKEN` | local runs | none | The token itself; takes precedence over the secret. Not set on the deployed functions |
| `QUEUE_URL` | fetch | none | URL of the tweets queue |
| `STATE_TABLE` | fetch | none | Name of the checkpoint table |
| `TWEETS_TABLE` | process | none | Name of the tweets table |
| `MEDIA_BUCKET` | process | none | Name of the media bucket |
| `MAX_RESULTS` | fetch | `100` | Tweets per API page, 5 to 100 |
| `MAX_PAGES` | fetch | `5` | Maximum pages per run, at least 1 |
| `X_API_BASE_URL` | fetch | `https://api.x.com/2` | API base URL, for testing against a stub |
| `HTTP_TIMEOUT_SECONDS` | both | `10` | Timeout for API and media HTTP requests, must be above 0 |

Blank values count as unset. The template sets everything except `TWITTER_BEARER_TOKEN`, `X_API_BASE_URL` and `HTTP_TIMEOUT_SECONDS`.

## Deploying

Prerequisites:

- An AWS account and credentials configured for the CLI (for example with `aws configure` or SSO).
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) (`brew install aws-sam-cli` or `uv tool install aws-sam-cli`).
- uv, as above.
- An X API bearer token for an account with API access (see limitations).

Steps:

```bash
# 1. Build the Lambda bundle into build/lambda/ (Python 3.14, arm64 wheels, from uv.lock via --locked;
#    the console script bin/ and direct_url.json are stripped)
scripts/build_lambda.sh

# 2. Build (again) and deploy with the schedule off; sam shows the change set and asks
#    for confirmation. Enabling the schedule before the secret has a value would make
#    the first runs fail and trip the fetch alarm.
SCHEDULE_ENABLED=false scripts/deploy.sh

# 3. Store the token in the stack's secret (prompts silently if the variable is unset)
scripts/put_secret.sh

# 4. Enable the schedule
SCHEDULE_ENABLED=true scripts/deploy.sh
```

`scripts/deploy.sh` runs the build itself, so step 1 is only needed to inspect the bundle. A deploy that omits `SCHEDULE_ENABLED` resets it to the template default, `true`, so set it explicitly if you want the schedule to stay off. Defaults come from `samconfig.toml` (stack `twitter-timeline-pipeline`, region `us-east-1`, IAM capabilities, change set confirmation, and `resolve_s3` for the artifact bucket, so no flag is needed). Override values that contain spaces are quoted for you. To override the account or schedule, set variables for that run:

```bash
TWITTER_USERNAME=NASA SCHEDULE_EXPRESSION='rate(30 minutes)' scripts/deploy.sh
```

Extra arguments are passed to `sam deploy`, for example `scripts/deploy.sh --region eu-west-1`. Both `put_secret.sh` and `delete.sh` read `STACK_NAME` (default `twitter-timeline-pipeline`); set it, and `AWS_REGION` if needed, if you changed the stack name or region.

### Template parameters

| Parameter | Default | Meaning |
| --- | --- | --- |
| `TwitterUsername` | `NASA` | Account whose timeline is fetched |
| `ScheduleExpression` | `rate(1 hour)` | EventBridge schedule for the fetch run |
| `ScheduleEnabled` | `true` | Set to `false` to disable the schedule rule |
| `MaxPages` | `5` | Maximum timeline pages per run |
| `MaxResults` | `100` | Tweets per page (5 to 100) |
| `LogLevel` | `INFO` | Lambda application log level (`DEBUG`, `INFO`, `WARN`, `ERROR`); logs are JSON. SDK and HTTP library loggers stay at WARNING regardless |
| `AlarmTopicArn` | empty | Optional SNS topic notified by the alarms |

### Stack outputs

`QueueUrl`, `DeadLetterQueueUrl`, `TweetsTableName`, `StateTableName`, `MediaBucketName`, `BearerTokenSecretArn`, `FetchFunctionName`, `ProcessFunctionName`.

## Testing a deployment

Read the outputs into shell variables:

```bash
STACK=twitter-timeline-pipeline
out() { aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
FETCH=$(out FetchFunctionName); QUEUE=$(out QueueUrl); DLQ=$(out DeadLetterQueueUrl)
TABLE=$(out TweetsTableName); BUCKET=$(out MediaBucketName)
```

**1. Invoke the fetch function** (after `put_secret.sh`):

```bash
aws lambda invoke --function-name "$FETCH" \
  --cli-binary-format raw-in-base64-out \
  --payload file://events/fetch.json /dev/stdout
# or: sam remote invoke FetchFunction --stack-name "$STACK" --event-file events/fetch.json
```

The response is the run summary (`pages`, `tweets`, `enqueued`, `newest_id`, `truncated`). `events/fetch.json` is `{"username": "NASA"}`; change it to fetch another account.

**2. Check the queues** (the process function drains the main queue within seconds):

```bash
aws sqs get-queue-attributes --queue-url "$QUEUE" --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
aws sqs get-queue-attributes --queue-url "$DLQ" --attribute-names ApproximateNumberOfMessages
```

**3. Look at the stored Tweets:**

```bash
aws dynamodb scan --table-name "$TABLE" --max-items 5

# latest 10 Tweets of an account, newest first, through the GSI
aws dynamodb query --table-name "$TABLE" \
  --index-name username-created_at-index \
  --key-condition-expression 'username = :u' \
  --expression-attribute-values '{":u": {"S": "nasa"}}' \
  --no-scan-index-forward --limit 10

aws dynamodb get-item --table-name "$TABLE" \
  --key '{"tweet_id": {"S": "1850000000000000001"}}'
```

Usernames are stored lower case, so query with a lower-case name.

**4. List the media:**

```bash
aws s3 ls "s3://$BUCKET/media/" --recursive --human-readable | head
aws s3api head-object --bucket "$BUCKET" --key '<a key from the listing>'   # shows tweet-id, media-key, source-url metadata
```

**5. Read the logs:**

```bash
# the stack creates the log groups, so look up their generated names
for fn in "$FETCH" "$(out ProcessFunctionName)"; do
  group=$(aws lambda get-function-configuration --function-name "$fn" \
    --query LoggingConfig.LogGroup --output text)
  aws logs tail "$group" --since 15m
done
# or: sam logs --stack-name "$STACK" --name FetchFunction
```

A second invocation right away should enqueue nothing new, because the checkpoint is set.

**Local container runs.** With Docker running, `sam local invoke` runs the functions from the built bundle (`build/lambda/`, so run `scripts/build_lambda.sh` first). These use real AWS resources if you give them credentials and environment values:

```bash
sam local invoke FetchFunction --event events/fetch.json
sam local invoke ProcessFunction --event events/sqs.json
```

**Automated tests.** `tests/` holds unit tests for each module, a test of the template, and an end-to-end test that runs both handlers against each other. AWS is mocked with moto and HTTP with `httpx.MockTransport`, so `uv run pytest` needs no AWS account, credentials or network.

## Operations

Use the stack outputs from [Testing a deployment](#testing-a-deployment) (`$QUEUE`, `$DLQ`, `$FETCH`) in these runbooks.

**Redrive the DLQ.** An alarm on the DLQ means records failed 5 times. Read the process function's logs first (a malformed message will fail again, and a stale redelivery overwrites newer data), fix the cause, then move the messages back before the 14-day retention expires them:

```bash
DLQ_ARN=$(aws sqs get-queue-attributes --queue-url "$DLQ" --attribute-names QueueArn \
  --query Attributes.QueueArn --output text)
aws sqs start-message-move-task --source-arn "$DLQ_ARN"   # defaults to the original source queue
aws sqs list-message-move-tasks --source-arn "$DLQ_ARN"   # progress
```

**Backfill or re-fetch.** The scheduled run only reads Tweets newer than the checkpoint, and a first run reads at most `MAX_PAGES x MAX_RESULTS`. To fetch a specific range, run the CLI against the real stack (it needs the stack's `QUEUE_URL` and `STATE_TABLE`, a token and AWS credentials), with `--since-id` set to the Tweet ID before the range and a higher `--max-pages`:

```bash
export QUEUE_URL=$QUEUE STATE_TABLE=$(out StateTableName)
export BEARER_TOKEN_SECRET_ARN=$(out BearerTokenSecretArn)
uv run twitter-pipeline fetch --username NASA --since-id 1850000000000000000 --max-pages 32
```

Re-sent Tweets are written again over the same items, so a backfill is safe to repeat. Add `--dry-run` to preview without sending. The API exposes only an account's 3,200 most recent Tweets (800 with `exclude=replies`), so nothing older can be backfilled.

**Rotate the token.** Run `scripts/put_secret.sh` with the new token. Running fetch containers keep the old token for up to 15 minutes (the cache TTL) and drop it earlier on a 401/403, so no redeploy is needed. To switch immediately, invoke the fetch function after the 15 minutes or update its configuration to recycle the containers.

**Pause or resume the schedule.** Redeploy with `SCHEDULE_ENABLED=false scripts/deploy.sh` (and `true` to resume). For a quick stop without a deploy, disable the rule in the console or with `aws events disable-rule`; the next deploy resets it to the `ScheduleEnabled` value. Messages already queued are still processed while paused.

## Tearing down

```bash
scripts/delete.sh          # asks for confirmation; add --yes to skip
```

The script reads the table and bucket names from the stack outputs, runs `sam delete`, and prints the commands to remove the retained tweets table and media bucket (`aws dynamodb delete-table`, then `aws s3 rm ... --recursive` and `aws s3api delete-bucket`). Nothing retained is deleted unless you run those. The secret, queues, state table, functions and log groups are deleted with the stack.

## Assumptions and limitations

- X API access now requires a paid plan. Billing is currently pay-per-use, charged per post read, and rate limits depend on the plan; check current pricing before deploying. Cost scales with Tweets read per run, which is bounded by `MaxPages x MaxResults` and multiplied by the schedule frequency, so lower `MaxPages` or run less often to spend less.
- The first run backfills at most `MAX_PAGES x MAX_RESULTS` Tweets (500 by default). The API itself only exposes an account's 3,200 most recent Tweets (800 with `exclude=replies`).
- Tweets can be lost in the page-cap case: if a run reaches the page cap while a checkpoint is set, Tweets older than those fetched but newer than the checkpoint are skipped, and a warning is logged. Raise `MaxPages` or run more often.
- Large videos have no size or time budget yet. A batch that times out is retried and can end up in the DLQ, and the Tweets in it have no metadata stored until it is redriven. DLQ messages expire after 14 days, after which they are gone.
- A media key that is missing from the response's `includes` (deleted or withheld media) is dropped with a warning, so the Tweet is stored without that media entry.
- Writes are last-write-wins: a stale redelivery or DLQ redrive overwrites a newer item.
- Edited Tweets get a new ID and are stored as new items; the old ones remain. `edit_history_tweet_ids` links them.
- Deletions are not propagated: a Tweet deleted at the source stays in the table.
- Metrics (`public_metrics`) are a snapshot at fetch time and are not refreshed.
- Videos that only have HLS variants (no `video/mp4`) are recorded with status `skipped` and nothing is downloaded.
- Media whose download returns a 4xx other than 429, or a redirect, is recorded as `unavailable`; the Tweet is still stored. Media at a non-https URL or a host other than the two X CDN hosts is recorded as `skipped`. 429 and 5xx responses and timeouts are retried.
- One account is fetched per stack through `TwitterUsername`. The fetch event can override the username; names are normalized to lower case, with each account keeping its own checkpoint.
- The bearer token is app-only, so protected accounts and private data are not available.

## Python 3.14 features used

- Deferred evaluation of annotations (PEP 649/749): `TYPE_CHECKING`-only imports such as `SecretsManagerClient` and `SQSClient` are used in annotations without quoting.
- Unparenthesized multiple exceptions (PEP 758): `except KeyError, ValueError:` in `x_api.py`.
- `type` statement aliases (PEP 695): `TweetId`, `MediaStatus` and others.
- Generic function syntax (PEP 695): `get_number[N: (int, float)]` in `config.py`.
- `itertools.batched` to split messages into SQS batches of 10.
- `typing.override` on the `_IterReader` methods.
- Structural pattern matching (`match`) to choose a media download source.

## License

GPL-3.0. See [LICENSE](LICENSE).
