import os
import tweepy as tw
import pandas as pd
from google.cloud import bigquery


# your Twitter API key and API secret
my_api_key = os.environ["TW_API_KEY"]
my_api_secret = os.environ["TW_API_SECRET"]
account_name = os.environ["ACCOUNT_NAME"]

bq_table_id = "dataset_test.noticias_de_ayer" # or use format: "project_id.dataset_id.table_id"
since_id = "2022-01-03"

def load_to_bq(pd_dataframe):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        pd_dataframe, bq_table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(bq_table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), bq_table_id
        )
    )


def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    # request_json = request.get_json()


    # authenticate
    auth = tw.OAuthHandler(my_api_key, my_api_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    # search_query = "#covid19 -filter:retweets"

    # get tweets from the API
    tweets = api.user_timeline(screen_name=account_name, since_id=since_id)

    # store the API responses in a list
    tweets_copy = []
    # print(tweets)
    for tweet in tweets:
        # tmp_dict = { key: tweet[key] for key in ["text", "created_at", "retweet_count", "favorite_count", "source"] }
        tmp_dict = dict()
        tmp_dict["text"] = tweet.text
        tmp_dict["created_at"] = tweet.created_at
        tmp_dict["retweet_count"] = tweet.retweet_count
        tmp_dict["favorite_count"] = tweet.favorite_count
        tmp_dict["source"] = tweet.source
        tmp_dict["account_name"] = account_name
        tweets_copy.append(tmp_dict)

    print("Total Tweets fetched:", len(tweets_copy))

    tweets_df = pd.DataFrame.from_dict(tweets_copy)
    print(tweets_df)

    load_to_bq(tweets_df)



hello_world("nada")