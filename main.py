from elasticsearch import Elasticsearch

import config


es = Elasticsearch(hosts=[{"host": config.ELASTICSEARCH_HOSTNAME, "port": config.ELASTICSEARCH_PORT}])


if __name__ == '__main__':
    print(es.info())
