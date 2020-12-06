import pandas as pd
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
    tweets = pd.read_json('./data/policy.json')
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

    print(tweets.columns)

    return users, tweets
