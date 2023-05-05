import time
import requests
from pytwitter import Api
import argparse
import access_twitter

twitter_access_token = access_twitter.get_twitter_secret()['twitter_access_token']

twitter_api = Api(bearer_token=twitter_access_token)

parser = argparse.ArgumentParser(description='Pull tweets, metadata from Twitter')
parser.add_argument(
    '--username', 
    default='who', 
    help='define Twitter account username to pull tweets from', 
    required=False
)

args = parser.parse_args()

user_response = twitter_api.get_user(username=args.username)
user_id = user_response.data.id

timeline_response = twitter_api.get_timelines(
    user_id=user_id, 
    max_results=5
    )
## TODO: We now have only the most recent 10 tweets
# Later, if we want more, we can use the param max_results=100 
# and pagination: `since` `until` to get older tweets from the timeline
# use until= for the last tweet ID returned
# Or reduce to 5 recent tweets

def get_tweet(tweet_id: str) -> dict:
    headers = {
        "Content-Type": "application/json", 
        "Authorization": f"Bearer {twitter_access_token}"
    }
    payload = {
        'id': tweet_id,
        'tweet_fields': ["created_at", "attachments", "referenced_tweets"],
        'expansions': ["attachments.media_keys"],
        'media_fields': ["variants", "preview_image_url", "url"]
    }
    tweet_fields = "attachments,created_at,referenced_tweets"
    expansion_fields = "attachments.media_keys"
    media_fields = "media_key,preview_image_url,url,type,variants"
    
    url = f"https://api.twitter.com/2/tweets?ids={tweet_id}&tweet.fields={tweet_fields}&expansions={expansion_fields}&media.fields={media_fields}"
    
    try:
        r = requests.get(url, headers=headers, json=payload)
        resp_data = r.json()
        
        if resp_data:
            d_tweet = dict()
            d_tweet['text'] = resp_data['data'][0]['text']
            d_tweet['created_at'] = resp_data['data'][0]['created_at']
            try:
                d_tweet['media_key'] = resp_data['includes']['media'][0]['media_key']
                d_tweet['media_url'] = resp_data['includes']['media'][0]['variants'][0]['url']
            except Exception as e:
                print('bypassing:', str(e)) 
            try:
                d_tweet['referenced_tweets'] = resp_data['data'][0]['referenced_tweets']
            except Exception as e:
                print('bypassing:', str(e)) 
    except Exception as e:
        print('error:', str(e))    
    return d_tweet

def collect_tweets(resp: dict) -> dict:
    d_tweets = dict()

    for item in resp.data:
        tweet_id = item.id
        tweet_params = get_tweet(tweet_id)
        d_tweets[tweet_id] = [tweet_params]
        time.sleep(1)
    
    return d_tweets

if __name__ == 'main':
    collect_tweets(timeline_response)
        
# next: `created_at`, `text`, `referenced_tweets` , `media`, [done]
# use perhaps a get_tweet or similar function on each of the tweets [done]
# then: after pull of twitter data is working 
# set up Lambda: set up basic deploy with Chalice --> [REST done]
# SQS --> [SQS created, SQS Chalice in progress]
# S3 --> [S3 created, S3 Chalice in progress]
# i.e. get the Chalice readme working
# create cloudformation for dyanamo db set up with table.
# create a schema in source code for dyanmo db.
# metadata goes to dynamo db, media to S3.
# keep work commited in git