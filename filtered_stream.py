import requests
import os
import json
import psycopg2
import pandas


# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

# Insert query name for creating tweet id table
query_name = 'Curfew'



def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "-is:retweet #Curfew", "tag": "hashtag curfew"},
        {"value": "-is:retweet curfew", "tag": "curfew"},
        {"value": "-is:retweet lockdown", "tag": "lockdown"},

    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))

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
            '''Create Table IF NOT EXISTS Query_{} (Tweet_id BIGINT PRIMARY KEY REFERENCES TwitterTweet(Tweet_Id));'''.format(query_name))

    # Execute SQL commands
    for command in commands:
        # Create tables
        cur.execute(command)
    conn.commit()


def get_stream(cur, conn):

    field_params = {
        'user.fields':'created_at,description,id,location,name,public_metrics',
        'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld'}

    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, params=field_params, stream=True
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            data = json_response['data']
            p_metrics = data['public_metrics']
            print(p_metrics['retweet_count'])
           
            
            # insert user information
            command = '''INSERT INTO TwitterUser (user_id) VALUES (%s) ON CONFLICT
                 (User_Id) DO NOTHING;'''
            cur.execute(command,(data['author_id'],))

            # insert tweet information
            command = '''INSERT INTO TwitterTweet (Tweet_Id, Tweet, Retweet_Count, Author_Id) VALUES (%s,%s,%s,%s);'''
            cur.execute(command,(data['id'], data['text'], p_metrics['retweet_count'], data['author_id']))

            command = '''INSERT INTO TwitterEntity (Id, Tweet_Id, Created_At) VALUES (%s,%s,%s) ON CONFLICT (Id) DO NOTHING;'''
            cur.execute(command,(data['author_id'], data['id'], data['created_at']))

            # insert tweet ids relations
            command = '''INSERT INTO Query_{} (Tweet_Id) VALUES (%s) ON CONFLICT
                 (Tweet_Id) DO NOTHING;'''.format(query_name)
            cur.execute(command,(data['id'],))
            conn.commit()

    cur.close()
    conn.close()
    
def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    cur, conn = connectdb()
    table_creation(cur, conn)
    get_stream(cur, conn)


if __name__ == "__main__":
    main()