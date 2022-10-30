import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

# functionize for dag
def run_twitter_etl():
    # get twitter credentials from file
    f = open("credentials.json")
    twitter_credentials = json.load(f)

    # authenticate twitter
    auth = tweepy.OAuthHandler(twitter_credentials['api_key'], twitter_credentials['api_secret'])
    auth.set_access_token(twitter_credentials['access_key'], twitter_credentials['access_secret'])

    # create API object
    api = tweepy.API(auth)

    # initialize user attributes
    tweets = api.user_timeline(screen_name='@FreisITG',
                                count=200,
                                include_rts=False,
                                tweet_mode='extended'
                                )

    # perform tweet pull
    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name, 
                        "text": text,
                        "favorite_count": tweet.favorite_count,
                        "retweet_count": tweet.retweet_count,
                        "created_at": tweet.created_at
                        }

        tweet_list.append(refined_tweet)

    # convert scraped data to pandas dataframe
    df = pd.DataFrame(tweet_list)

    # save tweet data to file
    df.to_csv("s3://gabsanti_airflow_bucket/twitter_etl/freisitg_twitter_data.csv")



