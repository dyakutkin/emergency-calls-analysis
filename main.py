import csv
import settings

import requests

from dateutil.parser import parse as parse_datetime
from pprint import pprint
from collections import defaultdict, OrderedDict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

es = Elasticsearch(hosts=[{'host': settings.ELASTICSEARCH_HOSTNAME, 'port': settings.ELASTICSEARCH_PORT}])

INDEX_NAME = 'calls'


def get_actions(filename=settings.DATASET_FILENAME):
    with open(filename) as csv_file:
        reader = csv.DictReader(csv_file)
        for index, row in enumerate(reader):
            row['date'] = parse_datetime(row.pop('timeStamp')).strftime("%Y-%m-%dT%H:%M")
            data = {
                '_op_type': 'index',
                '_index': INDEX_NAME,
                '_type': 'call',
                '_id': index,
            }
            data.update(row)
            yield data


def initialize_data(filename=settings.DATASET_FILENAME):
    try:
        es.search(index=INDEX_NAME)
    except NotFoundError:
        print('Importing data from {}...'.format(filename))
        bulk(es, get_actions(filename))
        print('Import succeeded.')


if __name__ == '__main__':
    initialize_data()
    get_actions(settings.DATASET_FILENAME)
    r = requests.get('http://elasticsearch:9200/calls/_search', json={
        "aggregations": {
            "simpleDateHistogram": {
                "date_histogram": {
                    "field": "date",
                    "interval": "hour",
                }
            }
        }
    })
    results = defaultdict(int)
    items = r.json().get('aggregations').get('simpleDateHistogram').get('buckets')
    print(items[0])
    for item in items:
        time = parse_datetime(item.get('key_as_string'))
        results[time.hour] += item.get('doc_count')

    ordered_results = OrderedDict()
    for key in sorted(results, key=lambda key: results[key], reverse=True):
        ordered_results[key] = results[key]
    pprint(ordered_results)
