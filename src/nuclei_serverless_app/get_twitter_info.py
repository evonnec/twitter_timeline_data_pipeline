import time
import requests
import boto3
import io
import argparse
from pytwitter import Api
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
        
        bypass_exception = "Does Not Exist for Tweet ID, thus bypassing: "
        
        if resp_data:
            d_tweet = dict()
            d_tweet['text'] = resp_data['data'][0]['text']
            d_tweet['created_at'] = resp_data['data'][0]['created_at']
            try:
                d_tweet['media_key'] = resp_data['includes']['media'][0]['media_key']
                d_tweet['media_url'] = resp_data['includes']['media'][0]['variants'][0]['url']
            except Exception as e:
                print(bypass_exception, str(e)) 
            try:
                d_tweet['referenced_tweets'] = resp_data['data'][0]['referenced_tweets']
            except Exception as e:
                print(bypass_exception, str(e)) 
    except Exception as e:
        print('error: ', str(e))    
    return d_tweet

def get_media_from_tweet(tweets: dict) -> None:
    """Retrieve Media From Tweet
    if it exists and push to S3, use the tweet_id as the folder

    Args:
        tweets (dict): 
        
    Returns: None
    """
    s3_client = boto3.client('s3')
    bucket_name = "twitter-media.nuclei-serverless-app"
    #TODO: use environment variable for bucket_name i.e. S3_BUCKET from deploy.sh
    s3_path = "media"
    bypass_exception = "Does Not Exist for Tweet ID, thus bypassing: "
    for item in tweets:
        try:
            response = requests.get(tweets[item][0]['media_url'])
            s3_client.upload_fileobj(
                io.BytesIO(response.content), 
                bucket_name, 
                f'{s3_path}/{item}'
            )
        except Exception as e:
            print(bypass_exception, str(e))
    
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
        d_tweets[tweet_id] = [tweet_params]
        time.sleep(1)
        # Sleep in order to avoid Too Many Requests error for now
        # TODO: However instead of sleep, perhaps use retries
        # https://stackoverflow.com/questions/15431044/can-i-set-max-retries-for-requests-request
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
        
    return d_tweets

if __name__ == '__main__':
    tweet_data = collect_tweets(timeline_response)
    tweet_media = get_media_from_tweet(tweet_data)
    print(tweet_data)
    print(tweet_media)
        
    # dynamo_client = boto3.client('dynamodb')
    # DynamoDB.Client.batch_execute_statement(**kwargs)
    # dynamo_client.batch_write_item(tweet_data)