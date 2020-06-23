# import packages
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
from slistener import SListener
from urllib3.exceptions import ProtocolError
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from keys_secret import consumer_key, consumer_secret
from keys_secret import access_token, access_token_secret

# consumer key authentication
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# set up the API with the authentication handler
api = API(auth)

# set up words to hear
keywords_to_hear = ['aadhaar',
                    'aadhar',
                    'adhaar',
                    ]

# instantiate the SListener object
listen = SListener(api)

# instantiate the stream object
stream = Stream(auth, listen)

# # create a engine to the database
#engine = create_engine("sqlite:///tweets.sqlite")
# # if the database does not exist
#if not database_exists(engine.url):
#     # create a new database
#    create_database(engine.url)

# begin collecting data
while True:
    # maintain connection unless interrupted
    try:
        stream.filter(track=keywords_to_hear)
    # reconnect automantically if error arise
    # due to unstable network connection
    except (ProtocolError, AttributeError):
        continue
