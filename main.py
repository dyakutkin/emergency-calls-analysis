import csv
import settings

import pygeohash

from dateutil.parser import parse as parse_datetime
from geopy.geocoders import Nominatim
from pprint import pprint
from collections import defaultdict, OrderedDict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import NotFoundError

es = Elasticsearch(hosts=[{
    'host': settings.ELASTICSEARCH_HOSTNAME,
    'port': settings.ELASTICSEARCH_PORT}])

INDEX_NAME = 'calls'


def get_actions(filename=settings.DATASET_FILENAME):
    with open(filename) as csv_file:
        reader = csv.DictReader(csv_file)
        for index, row in enumerate(reader):
            row['date'] = parse_datetime(row.pop('timeStamp')).strftime("%Y-%m-%dT%H:%M")
            row['location'] = {
                'lat': float(row.pop('lat')),
                'lon': float(row.pop('lng'))
            }
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
        es.indices.create(index=INDEX_NAME, body={
            "mappings": {
                "call": {
                    "properties": {
                        "location": {
                            "type": "geo_point",
                        }
                    }
                }
            }
        })
        print('Importing data from {}...'.format(filename))
        bulk(es, get_actions(filename))
        print('Import succeeded.')


def get_dict_ordered_by_value(dict_):
    ordered_results = OrderedDict()
    for key in sorted(dict_, key=lambda key: dict_[key], reverse=True):
        ordered_results[key] = dict_[key]
    return ordered_results


def fetch_busiest_hours():
    items = es.search(index=INDEX_NAME, doc_type='call', body={
        "aggregations": {
                "simpleDateHistogram": {
                    "date_histogram": {
                        "field": "date",
                        "interval": "hour",
                    }
                }
            }
    })['aggregations']['simpleDateHistogram']['buckets']
    results = defaultdict(int)

    for item in items:
        time = parse_datetime(item['key_as_string'])
        results[time.hour] += item['doc_count']

    return results


def fetch_busiest_regions():
    items = es.search(index=INDEX_NAME, doc_type='call', body={
        "aggregations": {
            "towns": {
                "geohash_grid": {
                    "field": "location",
                    "precision": 5
                }
            }
        }
    })['aggregations']['towns']['buckets']

    results = defaultdict(int)
    geolocator = Nominatim()

    for item in items:
        lat, lon = pygeohash.decode(item['key'])
        location = geolocator.reverse('{}, {}'.format(lat, lon))
        results[location.address] += item['doc_count']

    return results


if __name__ == '__main__':
    initialize_data()

    pprint(get_dict_ordered_by_value(fetch_busiest_hours()))
    pprint(get_dict_ordered_by_value(fetch_busiest_regions()))