import pandas as pd
import sqlite3
import re
import numpy as np
from us_state_abbrev import us_state_abbrev, abbrev_us_state
from city_to_state import city_to_state_dict


#helper functions
def contains_state(loc_string):
    '''
    parameters:
        loc_string: user provided location data from user['location']

    returns:
        state name if found, otherwise None

    '''
    #look for two consecutive capital letters
    try:
        #check if string contains a state name
        state = [state for state in us_state_abbrev.keys() if (str.lower(state) in str.lower(loc_string))]
        return us_state_abbrev[state[0]]
    except:
        pass
    try:
        #check if string contains a state abbreviation
        regex = r'\b[A-W][A-Y]\b'
        abbrev = re.search(regex, loc_string)
        return abbrev.group()
    except:
        pass
    try:
        #check if string contains a city
        city = [city for city in city_to_state_dict.keys() if (str.lower(city) in str.lower(loc_string))]
        return us_state_abbrev[city_to_state_dict[city[0]]]
    except:
        pass

def lookup(dictionary, val):
    try:
        return dictionary[val]
    except:
        return None

def tweets_json_to_dfs(filepath):
    '''
    parameters:
        filepath: string containing path to a json file containing tweets

    returns:
        users: dataframe containing trimmed user data
        tweets: dataframe containing trimmed tweets
    '''

    target_user_cols = [
        'id',
        'name',
        'screen_name',
        'location',
        'url',
        'description',
        'protected',
        'verified',
        'followers_count',
        'friends_count',
        'favourites_count',
        'statuses_count',
        'created_at',
        'utc_offset',
        'time_zone',
        'geo_enabled',
        'lang',
        'default_profile',
        'default_profile_image']

    target_tweet_cols = [
        'created_at',
        'id',
        'user_id',
        'text',
        'in_reply_to_status_id',
        'in_reply_to_user_id',
        'is_quote_status',
        'quote_count',
        'reply_count',
        'retweet_count',
        'favorite_count',
        'lang']

    #may be useful later (tweets)
    #entities
    #truncated
    #entities
    #geo
    #place
    #coordinates
    #extended tweet


    #load data
    tweets = pd.read_json(filepath)
    userraw = pd.DataFrame(tweets['user'].tolist())

    #users df
    #drop duplicate users 
    users = userraw.drop_duplicates(subset=['id'])[target_user_cols]
    #determine state by location
    users['state'] = [lookup(abbrev_us_state, abbrev) for abbrev in [contains_state(location) for location in users['location']]]
    #drop the users whose state we couldn't determine (to minimize db filesize)
    users = users.dropna(subset=['state'])

    #tweets df
    #add user ids
    tweets['user_id'] = userraw['id']
    #drop tweets from users whose state we couldn't determine
    tweets = tweets[tweets['user_id'].isin(users['id'])]
    #replace text with full text if the tweet is an extended tweet
    tweets['full_text'] = [d.get('full_text') if type(d) == dict else None for d in tweets['extended_tweet']]
    tweets['text'] = np.where(tweets['truncated'], tweets['full_text'], tweets['text'])
    tweets = tweets[target_tweet_cols]

    return users, tweets


def make_database(data_source, database):

    #make connection to new db
    conn = sqlite3.connect(database)
    c = conn.cursor()
    #load tweets
    users, tweets = tweets_json_to_dfs(data_source)
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