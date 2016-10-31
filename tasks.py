import time

import config

from invoke import task


@task
def wait_till_port_is_open(ctx):
    import socket
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((config.ELASTICSEARCH_HOSTNAME, config.ELASTICSEARCH_PORT))
        sock.close()
        if result == 0:
            return
        time.sleep(1)

@task
def run_dev(ctx):
    wait_till_port_is_open(ctx)
    ctx.run('python main.py')
