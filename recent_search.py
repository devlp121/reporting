import requests
import os
import json
import psycopg2


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

search_url = "https://api.twitter.com/2/tweets/search/recent"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
query_params = {
    'query': '(barclays ETF) OR (ETF newgold)',
    'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld',
    'user.fields':'created_at,description,id,location,name,public_metrics'}

query_name = 'recent_search_newgold'

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connectdb():
    conn = psycopg2.connect(host="172.18.0.2",database="superset",port=5432,user="superset",password="superset")

    cur = conn.cursor()

    return cur, conn

def table_creation(cur, conn):
    commands = (# Table 1
            '''Create Table IF NOT EXISTS TwitterUser (User_Id BIGINT PRIMARY KEY);''',
            # Table 2
            '''Create Table IF NOT EXISTS TwitterTweet (Tweet_Id BIGINT PRIMARY KEY,
                                         Tweet TEXT,
                                         Retweet_Count INT,
                                             Author_Id BIGINT,
                                             FOREIGN KEY (Author_Id)
                                                 REFERENCES TwitterUser(User_Id));''',
            # Table 3
            '''Create Table IF NOT EXISTS TwitterEntity (Id BIGINT PRIMARY KEY,
                                         Tweet_Id BIGINT,
                                         FOREIGN KEY (Tweet_Id)
                                          REFERENCES TwitterTweet(Tweet_Id),
                                         Created_At TIMESTAMP);''',
            #Table 4                                     
            '''Create Table IF NOT EXISTS {} (Tweet_id BIGINT PRIMARY KEY REFERENCES TwitterTweet(Tweet_Id));'''.format(query_name))

    # Execute SQL commands
    for command in commands:
        # Create tables
        cur.execute(command)
    conn.commit()

def ingest_db(response, cur, conn):

    for data in response['data']:
        try:
            p_metrics = data['public_metrics']
            print(data['text'])
            # insert user information
            command = '''INSERT INTO TwitterUser (user_id) VALUES (%s) ON CONFLICT
                    (User_Id) DO NOTHING;'''
            cur.execute(command,(data['author_id'],))

            # insert tweet information
            command = '''INSERT INTO TwitterTweet (Tweet_Id, Tweet, Retweet_Count, Author_Id) VALUES (%s,%s,%s,%s) ON CONFLICT (Tweet_Id) DO NOTHING;'''
            cur.execute(command,(data['id'], data['text'], p_metrics['retweet_count'], data['author_id']))

            command = '''INSERT INTO TwitterEntity (Id, Tweet_Id, Created_At) VALUES (%s,%s,%s) ON CONFLICT (Id) DO NOTHING;'''
            cur.execute(command,(data['author_id'], data['id'], data['created_at']))

            # insert tweet ids relations
            command = '''INSERT INTO {} (Tweet_Id) VALUES (%s) ON CONFLICT
                    (Tweet_Id) DO NOTHING;'''.format(query_name)
            cur.execute(command,(data['id'],))
        except:
            print ("Error on INSERT, assuming format error!")
        conn.commit()

    cur.close()
    conn.close()

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():
    json_response = connect_to_endpoint(search_url, query_params)
    cur, conn = connectdb()
    table_creation(cur, conn)
    ingest_db(json_response, cur, conn)


if __name__ == "__main__":
    main()
