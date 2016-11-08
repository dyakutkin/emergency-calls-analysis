import csv
import settings

from dateutil.parser import parse as parse_datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

es = Elasticsearch()

INDEX_NAME = 'calls'


def get_actions(filename=settings.DATASET_FILENAME):
    with open(filename) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            row['timestamp'] = parse_datetime(row.pop('timeStamp'))
            yield {
                '_op_type': 'create',
                '_index': INDEX_NAME,
                '_type': 'call',
                'body': row
            }


def initialize_data(filename=settings.DATASET_FILENAME):
    try:
        es.search(index=INDEX_NAME)
    except NotFoundError:
        print('Importing data from {}...'.format(filename))
        bulk(es, get_actions(filename))
        print('Import succeeded.')


if __name__ == '__main__':
    initialize_data()
    print(es.search(index=INDEX_NAME, doc_type='document', body={
        "aggregations": {
            "simpleDatehHistogram": {
                "date_histogram": {
                    "field": "timestamp",
                    "interval": "minute",
                }
            }
        }
    }))
