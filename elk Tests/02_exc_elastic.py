from aa_elasticsearch_client import elasticsearch_client

if __name__ == '__main__':
    es_client = elasticsearch_client()
    # Note: run me only once
    # es_client.create_index()

    # test_create_new_record()
    # es_client.get_data()
    es_client.get_index_data()