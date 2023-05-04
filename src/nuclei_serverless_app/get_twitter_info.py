import os
import time
import requests
from requests.auth import HTTPBasicAuth
from pytwitter import Api
import argparse

twitter_access_token = os.environ["TWITTER_ACCESS_TOKEN"] 
# decomm and use secrets manager for bearer token

twitter_api = Api(bearer_token=twitter_access_token)

headers = {
    "Content-Type": "application/json", 
    "Authorization": f"Bearer {twitter_access_token}"
}

parser = argparse.ArgumentParser(description='Pull tweets, metadata from Twitter')
parser.add_argument(
    '--username', 
    default='who', 
    help='define Twitter account username to pull tweets from', 
    required=True
)

args = parser.parse_args()

user_response = twitter_api.get_user(username=args.username)
user_id = user_response.data.id

"""
:param user_id: The id for target user.
:param start_time: Oldest or earliest UTC timestamp for tweets, format YYYY-MM-DDTHH:mm:ssZ.
:param end_time: Newest or most recent UTC timestamp for tweets, format YYYY-MM-DDTHH:mm:ssZ.
:param since_id: Greater than (that is, more recent than) tweet id for response. Exclude this since_id.
:param until_id: Less than (that is, older than) tweet id for response. Exclude this until_id.
:param max_results: The maximum number of results to be returned per page. Number between 5 and the 100.
By default, each page will return 10 results.
:param pagination_token: Token for the pagination.
:param tweet_fields: Fields for the tweet object.
:param exclude: Fields for types of Tweets to exclude from the response.
:param expansions: Fields for the expansions.
:param user_fields: Fields for the user object, Expansion required.
:param media_fields: Fields for the media object, Expansion required.
:param place_fields: Fields for the place object, Expansion required.
:param poll_fields: Fields for the poll object, Expansion required.
:param return_json: Type for returned data. If you set True JSON data will be returned.
:return: Response instance or json.
"""

timeline_response = twitter_api.get_timelines(
    user_id=user_id, 
    max_results=5
    )
    # user_fields=["created_at", "text", "referenced_tweets", "media"],
    # media_fields=["media"]
    # tweet_fields=["created_at", "text", "referenced_tweets", "media"]
    #     expansions="referenced_tweets.id,attachments.media_keys",
    #     tweet_fields="id,text,created_at,referenced_tweets,attachments"
    # 
    # example:
    # {"data":[{"text":"The plan places strong emphasis on:\n\uD83D\uDD39 #COVID19 disease prevention, 
    # control and management\n\uD83D\uDD39 addressing post-COVID condition (#longCOVID)\n\uD83D\uDD39 
    # strengthening health systems\n\n\uD83D\uDCCC https://t.co/5lqS5EWwqz https://t.co/qAcaNyc6l6",
    # "author_id":"14499829",
    # "id":"1653774434526789632",
    # "public_metrics":{"retweet_count":10,"reply_count":7,"like_count":38,"quote_count":0,"impression_count":35584},
    # "edit_history_tweet_ids":["1653774434526789632"],
    # "attachments":{"media_keys":["3_1653773802730340352"]},
    # "created_at":"2023-05-03T14:52:05.000Z",
    # "referenced_tweets":[{"type":"replied_to","id":"1653767965404266500"}]}]}
## TODO: We now have only the most recent 10 tweets
# Later, if we want more, we can use the max_results=100 
# and pagination: `since` `until` to get older tweets from the timeline
# use until= for the last tweet ID returned
# Or reduce to 5 recent tweets

def get_tweets(tweet_id):
    payload = {
        'id': tweet_id,
        'tweet_fields': ["created_at", "attachments", "referenced_tweets"],
        'expansions': ["attachments.media_keys"],
        'media_fields': ["variants", "preview_image_url", "url"]
    }
    tweet_fields = "attachments,created_at,referenced_tweets"
    expansion_fields = "attachments.media_keys"
    media_fields = "media_key,preview_image_url,url,type,variants"
    # https://api.twitter.com/2/users/:id/timelines/reverse_chronological?tweet.fields=created_at&expansions=author_id&user.fields=created_at&max_results=5
    # reverse_chronological is time ascending?
    url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields={tweet_fields}&expansions={expansion_fields}&media.fields={media_fields}"
    try:
        r = requests.get(url, headers=headers, json=payload)
        resp_data = r.json()
    except Exception as e:
        print('error', str(e))
        print(resp_data)
    return resp_data
    
dTweets = dict()

for item in timeline_response.data:
    tweet_id = item.id
    tweet_text = item.text
    tweet_params = get_tweets(tweet_id)
    dTweets[tweet_id] = [tweet_text] # add tweet_params

        
# next: `created_at`, `text`, `referenced_tweets` , `media`, 
# use perhaps a get_tweet or similar function on each of the tweets
# then: after all of twitter is working
# set up Lambda: set up basic deploy with Chalice --> 
# SQS --> lambda --> metadata goes to dynamo db, media to S3.
# i.e. get the Chalice readme working
# keep work commited in git