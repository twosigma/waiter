import logging

import requests

from waiter import terminal, http
from waiter.querying import get_token
from waiter.util import guard_no_cluster, print_info


def print_post_result(resp):
    """Prints the result of a token POST"""
    resp_json = resp.json()
    if 'message' in resp_json:
        message = resp_json['message']
        print_info(f'{message}.')
        return

    if 'waiter-error' in resp_json and 'message' in resp_json['waiter-error']:
        message = resp_json['waiter-error']['message']
    else:
        message = 'Encountered unexpected error'
    raise Exception(f'{message}.')


def post_failed_message(cluster_name, reason):
    """Generates a failed token post message with the given cluster name and reason"""
    return f'Token post {terminal.failed("failed")} on {cluster_name}:\n{terminal.reason(reason)}'


def create_or_update(cluster, token_name, token_fields):
    """Creates (or updates) the given token on the given cluster"""
    cluster_name = cluster['name']
    cluster_url = cluster['url']

    token = get_token(cluster, token_name)
    try:
        print_info('Attempting to post on %s cluster...' % terminal.bold(cluster_name))
        json_body = token if token else {}
        json_body.update(token_fields)
        resp = http.post(cluster, 'token', json_body, params={'token': token_name})
        print_post_result(resp)
        if resp.status_code == 201:
            return 0
    except requests.exceptions.ReadTimeout as rt:
        logging.exception(rt)
        print_info(terminal.failed(
            f'Encountered read timeout with {cluster_name} ({cluster_url}). Your post may have completed.'))
        return 1
    except IOError as ioe:
        logging.exception(ioe)
        reason = f'Cannot connect to {cluster_name} ({cluster_url})'
        message = post_failed_message(cluster_name, reason)
        print_info(f'{message}\n')


def create(clusters, args, _):
    """Creates (or updates) a Waiter token"""
    guard_no_cluster(clusters)
    logging.debug('create args: %s' % args)
    cluster = clusters[0]
    token_name = args.pop('token')
    token_fields = args
    return create_or_update(cluster, token_name, token_fields)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    submit_parser = add_parser('create', help='create token')
    submit_parser.add_argument('--name', '-n', help='name of service')
    submit_parser.add_argument('--version', '-v', help='version of service')
    submit_parser.add_argument('--cmd', '-C', help='command to start service')
    submit_parser.add_argument('--cmd-type', '-t', help='command type of service (e.g. "shell")', dest='cmd-type')
    submit_parser.add_argument('--cpus', '-c', help='cpus to reserve for service', type=float)
    submit_parser.add_argument('--mem', '-m', help='memory to reserve for service', type=int)
    submit_parser.add_argument('--ports', help='number of ports to reserve for service', type=int)
    submit_parser.add_argument('token', nargs=1)
    return create
