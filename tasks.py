import time
import os

import settings

from invoke import task



@task
def wait_till_port_is_open(ctx):
    import socket
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((settings.ELASTICSEARCH_HOSTNAME, settings.ELASTICSEARCH_PORT))
        sock.close()
        if result == 0:
            return
        time.sleep(1)


@task
def fetch_data(ctx):
    if not os.path.exists(settings.DATASET_FILENAME):
        ctx.run('wget {}'.format(settings.DATASET_LINK))


@task
def run_dev(ctx):
    wait_till_port_is_open(ctx)

    fetch_data(ctx)
    ctx.run('python main.py')
