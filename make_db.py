import sqlite3
import pandas as pd
from helpers import tweets_json_to_dfs

conn = sqlite3.connect("easProj.db")
c = conn.cursor()

#load tweets
users, tweets = tweets_json_to_dfs('./data/policy.json')
c.execute("DROP TABLE IF EXISTS users")
c.execute("DROP TABLE IF EXISTS tweets")

users_sql = """CREATE TABLE users (
id INTEGER PRIMARY KEY,
name TEXT,
screen_name TEXT,
location TEXT,
url TEXT,
description TEXT,
protected INTEGER,
verified INTEGER,
followers_count INTEGER,
friends_count INTEGER,
favourites_count INTEGER,
statuses_count INTEGER,
created_at DATETIME,
utc_offset TEXT,
time_zone TEXT,
geo_enabled INTEGER,
lang TEXT, 
default_profile INTEGER,
default_profile_image INTEGER,
state INTEGER)"""

tweets_sql = """CREATE TABLE tweets (
created_at DATETIME,
id integer PRIMARY KEY,
user_id INTEGER,
text TEXT,
in_reply_to_status_id INTEGER,
in_reply_to_user_id INTEGER,
is_quote_status TEXT,
quote_count INTEGER,
reply_count INTEGER,
retweet_count INTEGER,
favorite_count INTEGER,
lang TEXT,
FOREIGN KEY (user_id) references users(id))"""

conn.execute(users_sql)
conn.execute(tweets_sql)
users.to_sql(name = 'users', 
						 con = conn,
						 if_exists = 'append', 
						 index = False)

tweets.to_sql(name= 'tweets',
					    con = conn,
					    if_exists = 'append',
					    index = False)

conn.commit()

conn.close()
