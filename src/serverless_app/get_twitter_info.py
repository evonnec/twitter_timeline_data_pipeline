import requests
import time
import uuid
import boto3
import argparse
import logging
import os
from pytwitter import Api

# import access_twitter

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
TWITTER_ACCESS_TOKEN = os.getenv("TWITTER_ACCESS_TOKEN")
AWS_STORAGE_BUCKET_NAME = os.getenv("AWS_STORAGE_BUCKET_NAME")
TWITTER_USER = "who"
DYANMO_DB_TABLENAME = os.getenv("DYANMO_DB_TABLENAME")

parser = argparse.ArgumentParser(
    description="Pull tweets, metadata from Twitter, push tweet metadata to dynamoDB, \
        push media to S3"
)
parser.add_argument(
    "--username",
    default=TWITTER_USER,
    help="define Twitter account username to pull tweets from",
    required=False,
)
parser.add_argument(
    "--bucketname",
    default=AWS_STORAGE_BUCKET_NAME,
    help="enter the S3 bucket name",
    required=False,
)
parser.add_argument(
    "--dynamotablename",
    default=DYANMO_DB_TABLENAME,
    help="enter the dynamo tablename",
    required=False,
)

args = parser.parse_args()


def twitter_username_timelines(uname: str) -> dict:
    # bearer_token = access_twitter.get_twitter_secret()['twitter_access_token']
    twitter_api = Api(bearer_token=TWITTER_ACCESS_TOKEN)  # bearer_token=bearer_token
    user_response = twitter_api.get_user(username=uname)
    user_id = user_response.data.id

    timeline_response = twitter_api.get_timelines(user_id=user_id, max_results=10)
    return timeline_response


## TODO: We now have only the most recent 10 tweets
# Later, if we want more, we can use the param max_results=100
# Or if we want less, we can reduce to 5 most recent tweets

# To start getting tweets from the timeline in reverse_chronological order,
# pass the `reverse_chronological` parameter

# Additionally, can use pagination: `since` or `until` to get older tweets
# from the timeline and wrap in a try/except block to page through.
# use until=<tweet_id> for the last tweet ID returned so it keeps track of
# where the app is as it paginates.


def get_tweet(tweet_id: str) -> dict:
    """get_tweet
    gets the `tweet_fields` for a tweet_id: created_at and referenced_tweets
    additionally expands attachments.media_keys in `expansions`
    to get `includes` items in `media_fields`

    Args:
        tweet_id (str): numeric string, ~20 chars in length

    Returns:
        dict: {tweet_id: [{text, created_at, referenced_tweets, media_key, media_url}]}
        * referenced_tweets, media_key, media_url: in the return dict if it exists
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TWITTER_ACCESS_TOKEN}",
    }
    payload = {
        "id": tweet_id,
        "tweet_fields": ["created_at", "attachments", "referenced_tweets"],
        "expansions": ["attachments.media_keys"],
        "media_fields": ["variants", "preview_image_url", "url"],
    }
    tweet_fields = "attachments,created_at,referenced_tweets"
    expansion_fields = "attachments.media_keys"
    media_fields = "media_key,preview_image_url,url,type,variants"

    url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields={tweet_fields}&expansions={expansion_fields}&media.fields={media_fields}"

    try:
        r = requests.get(url, headers=headers, json=payload)
        resp_data = r.json()

        bypass_exception = "Does Not Exist for Tweet ID, thus bypassing: "

        if resp_data:
            d_tweet = dict()
            d_tweet["text"] = resp_data["data"][0]["text"]
            d_tweet["created_at"] = resp_data["data"][0]["created_at"]
            try:
                d_tweet["media_key"] = resp_data["includes"]["media"][0]["media_key"]
                d_tweet["media_url"] = resp_data["includes"]["media"][0]["variants"][0][
                    "url"
                ]
            except Exception as e:
                print(bypass_exception, str(e))
            try:
                d_tweet["referenced_tweets"] = resp_data["data"][0][
                    "referenced_tweets"
                ][0]
            except Exception as e:
                print(bypass_exception, str(e))
    except Exception as e:
        print("error: ", str(e))
    return d_tweet


def get_media_from_tweet_push_to_s3(tweets: dict, storage_bucket: str) -> None:
    """Retrieve Media From Tweet
    if it exists and push to S3, use the tweet_id as the folder

    Args:
        tweets (dict):

    Returns: None
    """
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3 = session.resource("s3")
    random_name = uuid.uuid4().hex
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    bypass_exception = "Does Not Exist for Tweet ID, thus bypassing: "
    for item in tweets:
        try:
            import pdb
            pdb.set_trace()

            tweet_id = requests.get(tweets[item]["tweet_id"])
            filename = f"{username}_{random_name}_{timestamp}_{tweet_id}"
            response = requests.get(tweets[item][0]["media_url"])
            filetype = requests.get(tweets[item][0]["file_type"])
            with open(response) as f_resp:
                s3.meta.client.put_object(
                    Body=f_resp.read(), #io.BytesIO(f_resp.content)
                    Key=filename + "_" + item,
                    Bucket=storage_bucket,
                    ContentType=filetype, # "application/json"
                )
            logging.info('File "%s" is transferred', filename)
        except Exception as e:
            print(bypass_exception, str(e))


def get_metadata_from_tweet_push_dynamo_db(tweets: dict, dynamo_table: str) -> None:
    """Retrieve metadata from Tweet and PUT to Dynamo DB

    Args:
        tweets (dict): tweets dict as input
    Return: None
    """
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    dynamo = session.resource("dynamodb")

    # TODO: get environment variable for table_name
    # api_url = os.getenv('API_URL')
    # TODO: need to pull API_URL from put.sh after deploying cloudformation
    # DynamoDB.Client.batch_execute_statement(**kwargs)
    for item in tweets:
        try:
            dynamo.put_item(TableName=dynamo_table, Item={item: {"M": tweets[item]}})
            # response = requests.put(api_url, data=payload)
            # To use requests.put, learn how to create `id` of PUT request
            # to set id == item
        except Exception as e:
            print("error: ", str(e))


def collect_tweets(resp: dict) -> dict:
    """Collect tweets from the input of a Tweet timeline

    Args:
        resp (dict): Tweet Timeline dict input

    Returns:
        dict: dict of the last ~10 tweets from the user timeline
    """
    d_tweets = dict()

    for item in resp.data:
        tweet_id = item.id
        tweet_params = get_tweet(tweet_id)
        d_tweets[tweet_id] = tweet_params
        time.sleep(1)
        # Sleep in order to avoid Too Many Requests error for now
        # TODO: However instead of sleep, perhaps use retries
        # https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html

    return d_tweets


if __name__ == "__main__":
    username = args.username
    bucket_name = args.bucketname
    dynamo_tablename = args.dynamotablename
    timeline_response = twitter_username_timelines(uname=username)
    tweet_data = collect_tweets(resp=timeline_response)
    get_media_from_tweet_push_to_s3(tweets=tweet_data, storage_bucket=bucket_name)
    # get_metadata_from_tweet_push_dynamo_db( \
    #     tweets=tweet_data, \
    #     dynamo_table=dynamo_tablename \
    # )
    print(tweet_data)
