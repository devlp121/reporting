import requests
import os
import json
import psycopg2
import pandas as pd

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

search_url = "https://api.twitter.com/2/tweets/counts/recent"

# Optional params: start_time,end_time,since_id,until_id,next_token,granularity
query_params = {'query': '(barclays ETF) OR (ETF newgold)','granularity': 'day'}

query_name = 'recent_count_newgold'


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentTweetCountsPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text) 
    return response.json()
    
def connectdb():
    conn = psycopg2.connect(host="172.18.0.2",database="superset",port=5432,user="superset",password="superset")

    cur = conn.cursor()

    return cur, conn

def table_creation(cur, conn):
    commands = (
            #Table Tweet Count                                 
            '''Create Table IF NOT EXISTS {} (Endtime TIMESTAMP PRIMARY KEY, Start TIMESTAMP, Tweet_count INT);'''.format(query_name))

    # Execute SQL commands
        # Create tables
    cur.execute(commands)
    conn.commit()

def ingest_db(response, cur, conn):

    for i in response['data']:
        # insert tweet ids relations
        command = '''INSERT INTO {} (endtime,start,tweet_count) VALUES (%s,%s,%s) ON CONFLICT
                (endtime) DO NOTHING;'''.format(query_name)
        cur.execute(command,(i['end'],i['start'],i['tweet_count']))
        conn.commit()


def main():
    df = connect_to_endpoint(search_url, query_params)
    cur, conn = connectdb()
    table_creation(cur, conn)
    ingest_db(df, cur, conn)
   

if __name__ == "__main__":
    main()