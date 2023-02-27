# file who provides elastic client capabilities:

from elasticsearch import Elasticsearch
import json
import datetime


class elasticsearch_client:
    client = None

    def __init__(self):
        self.client = Elasticsearch(['http://localhost:9200/'], verify_certs=True)


        print("Managed To Connect To ElasticSearch {}".format(self.client.ping()))

    def __del__(self):
        if self.client is not None:
            self.client.transport.close()


    def create_index(self, index_name='sentiment_twits'):
        created = False
        # index settings
        my_settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "my_type": {
                    "properties": {
                        "tweet_id": {
                            "type": "text"
                        },
                        "tweet_created_at": {
                            "type": "text"
                        },
                        "location": {
                            "type": "text"
                        },
                        "url": {
                            "type": "date"
                        },
                        "text": {
                            "type": "text"
                        },
                        "user_acount_created_at": {
                            "type": "text"
                        },
                        "user_id": {
                            "type": "integer"
                        },
                        "name": {
                            "type": "text"
                        },
                        "followers_count": {
                            "type": "text"
                        },
                        "friends_count": {
                            "type": "text"
                        },
                        "listed_count": {
                            "type": "text"
                        },
                        "current_ts": {
                            "type": "text"
                        },
                        "hour": {
                            "type": "integer"
                        },
                        "minute": {
                            "type": "integer"
                        },
                        "wordCount": {
                            "type": "integer"
                        },
                        "sentiment": {
                            "type": "float"
                        }
                    }
                }
            }
        }


        try:
            # if not self.client.exists(index_name):
            #     # Ignore 400 means to ignore "Index Already Exist" error.
            self.client.indices.create(index=index_name, ignore=400, body=my_settings)
            print('Created Index')
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created

    def store_record(self, id, record, index_name='sentiment_twits'):
        try:
            outcome = self.client.create(id=id, index=index_name, body=json.dumps(record).encode('utf_8'))
            print(outcome)
        except Exception as ex:
            print('Error in Storing New Record')
            print(str(ex))

    def get_data(self):
        try:
            outcome = self.client.indices.get_alias()
            print(outcome)
        except Exception as ex:
            print('Error in indexing data')
            print(str(ex))

    def get_index_data(self, index_name='sentiment_news'):
        try:
            result = self.client.search(
                index=index_name,
                body={
                    "query": {
                        "match_all": {}
                    }
                }
            )
            print(result)
        except Exception as ex:
            print('Error in indexing data')
            print(str(ex))

    @classmethod
    def elasticsearch_client(cls):
        pass