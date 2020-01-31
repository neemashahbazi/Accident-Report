import configparser

import tweepy as tw
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def get_tweets(search_param, date_since, tweet_count):
    # Initialize Variables
    config = configparser.RawConfigParser()
    config.read('twitter_config.properties')

    # Authenticating with Twitter
    auth = tw.OAuthHandler(config.get('APICreds', 'consumer_key'), config.get('APICreds', 'consumer_secret'))
    auth.set_access_token(config.get('APICreds', 'access_token'), config.get('APICreds', 'access_secret'))
    api = tw.API(auth, wait_on_rate_limit=True)

    # Filtering out Retweets
    search_words = search_param + " -filter:retweets"

    # Collect tweets
    return tw.Cursor(api.search, q=search_words, lang="en", since=date_since).items(tweet_count)


def read_tweets_from_tweeter(search_params, date_since, tweet_count):
    topic_name = 'tweeter_feed'

    # Get Tweets from Twitter
    tweets = []
    for search_param in search_params:
        try:
            tweets = get_tweets(search_param, date_since, tweet_count)
        except Exception as ex:
            print('Exception while connecting Twitter')
            print(str(ex))

        # Get Kafka Producer
        producer = connect_kafka_producer()

        # Iterate and print tweets
        for tweet in tweets:
            # Extract hashtags as single string
            hash_tags = ''
            for hash_tag in tweet.entities['hashtags']:
                hash_tags = hash_tags + "#" + hash_tag['text']

            # Combine data with " <-> " as delimiter
            message = (tweet.text.replace("\n", " ") + " <-> " + str(tweet.user.name) + " <-> " + str(
                tweet.user.location)
                       + " <-> " + str(tweet.geo) + " <-> " + str(hash_tags) + " <-> crash") # crash is a placeholder
            print("-> " + message)  # str(tweet.place)

            # Publish Message to Kafka
            publish_message(producer, topic_name, message)


# Call to run the program
read_tweets_from_tweeter(['#accident', '#crash'], '2019-12-06', 3)
