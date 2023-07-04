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

    # https://api.twitter.com/2/users/:id/timelines/reverse_chronological?tweet.fields=created_at&expansions=author_id&user.fields=created_at&max_results=5
    # reverse_chronological is time ascending?
