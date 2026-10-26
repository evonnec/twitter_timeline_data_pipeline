"""A stand-in for the X API and the X media CDN, for the integration tests.

Run it with ``python stub_server.py --port 8099 --public-url http://host:8099``; it
needs only the standard library. One server plays both roles: the Lambdas reach it
through ``X_API_BASE_URL`` (``<public-url>/2``) and, for media, through the URLs the
timeline hands out (``<public-url>/media/...``).

Endpoints:

- ``GET /2/users/by/username/<name>``: the user lookup (any name resolves).
- ``GET /2/users/<id>/tweets``: a three-Tweet timeline, newest first, honouring
  ``since_id`` (exclusive). Needs ``Authorization: Bearer <token>``, else 401.
- ``GET /media/<name>``: fixed JPEG-like bytes; ``Missing.jpg`` answers 404.
- ``GET /__requests``: every request seen so far, as JSON. ``POST /__reset`` clears it.
- ``GET /__health``: 200, for readiness checks.
"""

import argparse
import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar
from urllib.parse import parse_qs, urlsplit

TOKEN = "integration-test-token"
USER_ID = "2244994945"
USERNAME = "example_user"
PHOTO_BYTES = b"\xff\xd8\xff\xe0integration-test-jpeg"
PHOTO_TWEET = "1700000000000000003"
TEXT_TWEET = "1700000000000000002"
MISSING_MEDIA_TWEET = "1700000000000000001"
PHOTO_KEY = "3_1700000000000000101"
MISSING_KEY = "3_1700000000000000199"
TWEET_IDS = (PHOTO_TWEET, TEXT_TWEET, MISSING_MEDIA_TWEET)


def timeline(public_url: str, since_id: str | None) -> dict[str, Any]:
    def tweet(tweet_id: str, text: str, hour: int, *keys: str) -> dict[str, Any]:
        item: dict[str, Any] = {
            "id": tweet_id,
            "text": text,
            "created_at": f"2026-10-20T{hour:02d}:00:00.000Z",
            "author_id": USER_ID,
            "conversation_id": tweet_id,
            "lang": "en",
            "public_metrics": {"like_count": hour},
            "edit_history_tweet_ids": [tweet_id],
        }
        if keys:
            item["attachments"] = {"media_keys": list(keys)}
        return item

    tweets = [
        tweet(PHOTO_TWEET, "Photo post", 12, PHOTO_KEY),
        tweet(TEXT_TWEET, "Text only", 11),
        tweet(MISSING_MEDIA_TWEET, "Media that was deleted", 10, MISSING_KEY),
    ]
    if since_id:
        tweets = [t for t in tweets if int(t["id"]) > int(since_id)]
    if not tweets:
        return {"meta": {"result_count": 0}}
    wanted = {k for t in tweets for k in t.get("attachments", {}).get("media_keys", [])}
    media = [
        {
            "media_key": key,
            "type": "photo",
            "url": f"{public_url}/media/{name}",
            "width": 1200,
            "height": 800,
        }
        for key, name in ((PHOTO_KEY, "Photo1.jpg"), (MISSING_KEY, "Missing.jpg"))
        if key in wanted
    ]
    return {
        "data": tweets,
        "includes": {"media": media},
        "meta": {"result_count": len(tweets), "newest_id": tweets[0]["id"]},
    }


class StubApi:
    """The server's behaviour as a function of the request, apart from the sockets."""

    def __init__(self, public_url: str) -> None:
        self.public_url = public_url
        self.seen: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def handle(
        self, method: str, target: str, authorization: str | None
    ) -> tuple[int, bytes, str]:
        """``(status, body, content type)`` for one request."""
        parts = urlsplit(target)
        path, query = parts.path, parse_qs(parts.query)

        def reply(status: int, payload: Any) -> tuple[int, bytes, str]:
            return status, json.dumps(payload).encode(), "application/json"

        if method == "POST" and path == "/__reset":
            with self.lock:
                self.seen.clear()
            return HTTPStatus.OK, b"reset", "text/plain"
        if method != "GET":
            return reply(HTTPStatus.NOT_FOUND, {"title": "Not Found"})
        if path == "/__health":
            return HTTPStatus.OK, b"ok", "text/plain"
        if path == "/__requests":
            with self.lock:
                return reply(HTTPStatus.OK, list(self.seen))
        with self.lock:
            self.seen.append(
                {
                    "method": method,
                    "path": path,
                    "query": {k: v[0] for k, v in query.items()},
                    "authorization": authorization,
                }
            )
        if path.startswith("/media/"):
            if path.endswith("/Missing.jpg"):
                return HTTPStatus.NOT_FOUND, b"gone", "text/plain"
            return HTTPStatus.OK, PHOTO_BYTES, "image/jpeg"
        if authorization != f"Bearer {TOKEN}":
            return reply(HTTPStatus.UNAUTHORIZED, {"title": "Unauthorized"})
        if path.startswith("/2/users/by/username/"):
            name = path.rsplit("/", 1)[1]
            data = {"id": USER_ID, "name": "Example Account", "username": name}
            return reply(HTTPStatus.OK, {"data": data})
        if path == f"/2/users/{USER_ID}/tweets":
            since = query.get("since_id", [None])[0]
            return reply(HTTPStatus.OK, timeline(self.public_url, since))
        return reply(HTTPStatus.NOT_FOUND, {"title": "Not Found"})


class Handler(BaseHTTPRequestHandler):
    api: ClassVar[StubApi]

    def _serve(self) -> None:
        status, body, content_type = self.api.handle(
            self.command, self.path, self.headers.get("Authorization")
        )
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _serve
    do_POST = _serve

    def log_message(self, format: str, *args: Any) -> None:
        pass  # the request list at /__requests is the log


def make_server(
    port: int, public_url: str, host: str = "0.0.0.0"
) -> ThreadingHTTPServer:
    """A server on ``host:port`` whose timeline links media under ``public_url``."""
    handler = type("BoundHandler", (Handler,), {"api": StubApi(public_url)})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument(
        "--public-url",
        required=True,
        help="base URL the Lambdas use to reach this server, e.g. http://host.docker.internal:8099",
    )
    args = parser.parse_args()
    server = make_server(args.port, args.public_url.rstrip("/"))
    print(f"stub X API and CDN on :{args.port} as {args.public_url}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
