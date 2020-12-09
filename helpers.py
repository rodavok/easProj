import pandas as pd
import sqlite3
import re
from us_state_abbrev import us_state_abbrev, abbrev_us_state

def tweets_json_to_dfs(filepath):
    '''
    parameters:
        filepath: string containing path to a json file containing tweets

    returns:
        users: dataframe containing trimmed user data
        tweets: dataframe containing trimmed tweets
    '''
    #helper functions
    def contains_state(loc_string):
        '''
        parameters:
            loc_string: user provided location data from user['location']

        returns:
            state name if found, otherwise None

        currently I'm just looking if the abbreviation is in the location string
        
        would like to add checks for full state name or major cities, but 
        this function has to return the name of the state associated with the match,
         or None if no match is found

        '''
        #look for two consecutive capital letters
        regex = r'\b[A-W][A-Y]\b'
        try:
            x = re.search(regex, loc_string)
            return x.group()
        except:
            return None

    def lookup(dictionary, val):
        try:
            return dictionary[val]
        except:
            return None

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

    #may be useful later for tweets-
    #entities
    #truncated
    #entities
    #geo
    #place
    #coordinates
    #extended tweet


    #dont forget to add json to gitignore
    #load data
    tweets = pd.read_json(filepath)
    userraw = pd.DataFrame(tweets['user'].tolist())

    #users df
    users = userraw.drop_duplicates(subset=['id'])[target_user_cols]
    #I can write this better
    users['state'] = [lookup(abbrev_us_state, abbrev) for abbrev in [contains_state(location) for location in users['location']]]
    #drop the users whose state we couldn't determine (to minimize db filesize)
    users = users.dropna(subset=['state'])

    #tweets df
    tweets['user_id'] = userraw['id']
    #drop tweets from users whose state we couldn't determine
    tweets = tweets[tweets['user_id'].isin(users['id'])]
    tweets = tweets[target_tweet_cols]

    return users, tweets


def make_database(data_source, database)

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