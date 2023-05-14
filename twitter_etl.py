import os
from datetime import datetime
from io import StringIO

import pandas as pd
import tweepy
import boto3


def run_twitter_etl():
    """ Run twitter api for gettig tweets
    """

    # twitter credentials
    access_key = os.getenv('ACCESS_KEY')
    access_secret = os.getenv('ACCESS_SECRET')
    consumer_key = os.getenv('CONSUMER_KEY')
    consumer_secret = os.getenv('CONSUMER_SECRET')

    AWS_S3_BUCKET = 's3-bucket'
  
    # twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)
    # creating API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(
        screen_name = '@elonmusk',
        count = 200,
        include_rts = False,
        tweet_mode = 'extended'
    )

    tweet_list = []
    csv_buffer = StringIO()

    for tweet in tweets:
        text = tweet._json['full_text']

        refined_tweet = {
            'user': tweet.user.screen_name,
            'text': text,
            'retweet_count': tweet.retweet_count,
            'created_at': tweet.created_at
        }

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(
        AWS_S3_BUCKET,
        'elonmusk_tweets.csv'
    ).put(Body=csv_buffer.getvalue())