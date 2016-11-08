import csv
import settings

from elasticsearch_dsl.connections import connections
from models import Call


es = connections.create_connection(hosts=[
    {
        'host': settings.ELASTICSEARCH_HOSTNAME,
        'port': settings.ELASTICSEARCH_PORT,
    },
])

Call.init()


def initialize_data(filename=settings.DATASET_FILENAME):
    print('Importing data from {}...'.format(filename))
    with open(filename) as csv_file:
        reader = csv.DictReader(csv_file)
        for index, row in enumerate(reader):
            Call(**row).save()
        print('Import succeeded.')


if __name__ == '__main__':
    initialize_data()
    pass
