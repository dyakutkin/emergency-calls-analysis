import os

ELASTICSEARCH_HOSTNAME = os.getenv('ELASTICSEARCH_HOSTNAME', 'localhost')
ELASTICSEARCH_PORT = int(os.getenv('ELASTICSEARCH_PORT', '9200'))

DATASET_FILENAME = '911.csv'
DATASET_LINK = 'https://www.dropbox.com/s/nzxm5gdj6e63oe1/{}'.format(DATASET_FILENAME)

