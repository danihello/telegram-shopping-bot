import json
import tweepy
from kafka import KafkaProducer
from time import sleep

consumer_key = '9Kul4QgCP4QE7ydJvFrQcLFa4'
consumer_secret = 'ne0nWw4lmZFFhDgKcSEL9ZDtMISy6wRmfvkh8FciTRhvNPSgkz'
access_token = '1153221248039755776-5UMImVxLCzTZilh8ASrYqFErjZ73tB'
access_secret = 'pWhFC5Bfo2Lx6BLdh3i62vxieXxpppOWzNj4leB1jo4b1'


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='Cnt7-naya-cdh63:9092',
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.tweets = []

    def on_data(self, data):
        # data is the full *tweet* json data
        api_events = json.loads(data)

        # Gathring relevant values
        # Event-related values
        event_keys = ['created_at', 'id', 'text']
        twitter_events = {k: v for k, v in api_events.items()
                          if k in event_keys}
        twitter_events['tweet_created_at'] = twitter_events.pop('created_at')
        twitter_events['tweet_id'] = twitter_events.pop('id')
        # User-related values
        user_keys = ['id', 'name', 'created_at', 'location', 'url', 'protected', 'verified',
                     'followers_count', 'friends_count', 'listed_count', 'favourites_count',
                     'statuses_count', 'withheld_in_countries']
        user_events = {k: v for k, v in api_events['user'].items()
                       if k in user_keys}
        user_events['user_acount_created_at'] = user_events.pop('created_at')
        user_events['user_id'] = user_events.pop('id')

        # Marge dictioneries
        user_events.update(twitter_events)
        events = user_events

        # send data to kafka topic(s)
        self.producer.send('TweeterArchive', events)
        self.producer.send('TweeterData', events)
        self.producer.flush()

        print(events)
        sleep(10)

    def on_error(self, status_code):
        if status_code == 420:
            return False


def initialize():

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    stream = TwitterStreamListener()
    twitter_stream = tweepy.Stream(auth=api.auth, listener=stream)
    twitter_stream.filter(track=['Trump'], languages=['en'])


initialize()
# sleep(10)