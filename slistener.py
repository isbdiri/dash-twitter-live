# import packages
from tweepy.streaming import StreamListener
import json
import time
import sys
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
import preprocessor as p
import re
import emoji
import itertools
import string
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def clean_method2(tweet):
    #remove mentions and URLs
    p.set_options(p.OPT.URL, p.OPT.MENTION, p.OPT.HASHTAG, p.OPT.NUMBER)
    tweet=p.clean(tweet)
    #replace consecutive non-ASCII characters with a space
    tweet = re.sub(r'[^\x00-\x7F]+',' ', tweet)
    #take care of contractions and stray quote marks
    tweet = re.sub(r'’',"'", tweet)
    #words = tweet.split()
    tweet = re.sub(r":"," ", tweet)
    tweet = re.sub(r"n't"," not", tweet)
    #fix spellings
    tweet = ''.join(''.join(s)[:2] for _, s in itertools.groupby(tweet))
    #emojis conversion
    tweet = emoji.demojize(tweet)
    tweet = ' '.join(tweet.split())
    return tweet

def filter_method2(tweet):
    stop_words = set(stopwords.words('english'))
    stray_tokens=['amp', '`', "``", "'", "''", '"', "n't", "I", "i", ",00"]#stray words
    punct = r'[{}]'.format(string.punctuation)
    tweet=re.sub(punct,' ',tweet)
    tweet=re.sub(r'[0-9]',' ',tweet)
    tweet=re.sub(r'aadhar|adhaar|aadhaar|aadar|aadahar|aadaar|aadarcard|aadaahr|adhar',' aadhaar ',tweet)
    tweet = re.sub(r'(^|\s)[a-z]($|\s)',' ', tweet)
    tweet = re.sub(r'(^|\s)[a-z][a-z]($|\s)',' ', tweet)
    word_tokens = word_tokenize(tweet)
    #filter using NLTK library append it to a string
    filtered_tweet = [w for w in word_tokens if not w in stop_words]
    filtered_tweet = []
    #looping through conditions
    for w in word_tokens:
        #check tokens against stopwords and punctuations
        if w not in stop_words and w not in string.punctuation and w not in stray_tokens:
            w=w.lower()
            filtered_tweet.append(w)
    tweet=' '.join(filtered_tweet)
    tweet = re.sub(r'(^|\s)[a-z]($|\s)',' ', tweet)#re-removing single characters
    tweet = re.sub(r'(^|\s)[a][a]($|\s)',' ', tweet)#fixing for aadhaar
    return tweet

# inherit from StreamListener class
class SListener(StreamListener):

    # initialize the API and a counter for the number of tweets collected
    def __init__(self, api = None, fprefix = 'streamer'):
        self.api = api or API()
        self.cnt = 0
        # create a engine to the database
        #self.engine = create_engine('sqlite:///app/tweets.sqlite')
        # switch to the following definition if run this code locally
        self.engine = create_engine('sqlite:///tweets.sqlite')

    # for each tweet streamed
    def on_status(self, status):
        
        # increment the counter
        self.cnt += 1

        # parse the status object into JSON
        status_json = json.dumps(status._json)
        # convert the JSON string into dictionary
        status_data = json.loads(status_json)

        # initialize a list of potential full-text
        full_text_list = [status_data['text']]

        # add full-text field from all sources into the list
        if 'extended_tweet' in status_data:
            full_text_list.append(status_data['extended_tweet']['full_text'])
        if 'retweeted_status' in status_data and 'extended_tweet' in status_data['retweeted_status']:
            full_text_list.append(status_data['retweeted_status']['extended_tweet']['full_text'])
        if 'quoted_status' in status_data and 'extended_tweet' in status_data['quoted_status']:
            full_text_list.append(status_data['quoted_status']['extended_tweet']['full_text'])

        # only retain the longest candidate
        full_text = max(full_text_list, key=len)

        # extract time and user info
        tweets = {
            'created_at': status_data['created_at'],
            'text': full_text,
            #'user': status_data['user']['description']
        }

        # uncomment the following to display tweets in the console
        print("Writing tweet # {} to the database".format(self.cnt))
        print("Tweet Created at: {}".format(tweets['created_at']))
        print("Tweet Content:{}".format(tweets['text']))
        #print("User Profile: {}".format(tweets['user']))
        print()

        # convert into dataframe
        df=pd.DataFrame(tweets, index=[0])
        
        # convert string of time into date time obejct
        df['created_at'] = pd.to_datetime(df.created_at)
        df['text'] = df['text'].apply(clean_method2)
        df['text'] = df['text'].apply(filter_method2)

        # push tweet into database
        df.to_sql('tweets', con=self.engine, if_exists='append')

        with self.engine.connect() as con:
            con.execute("""
                        DELETE FROM tweets
                        WHERE created_at in(
                            SELECT created_at
                                FROM(
                                    SELECT created_at, strftime('%s','now') - strftime('%s',created_at) AS time_passed
                                    From tweets
                                    WHERE time_passed >= 10800))""")
        