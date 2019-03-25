import logging

import requests

from waiter import terminal, http_util
from waiter.querying import get_token
from waiter.util import guard_no_cluster, print_info, TRUE_STRINGS, FALSE_STRINGS, str2bool, response_message

BOOL_STRINGS = TRUE_STRINGS + FALSE_STRINGS

create_parser = None


def process_post_result(resp):
    """Prints the result of a token POST"""
    resp_json = resp.json()
    if 'message' in resp_json:
        message = resp_json['message']
        print_info(f'{message}.')
        return

    raise Exception(f'{response_message(resp_json)}.')


def post_failed_message(cluster_name, reason):
    """Generates a failed token post message with the given cluster name and reason"""
    return f'Token post {terminal.failed("failed")} on {cluster_name}:\n{terminal.reason(reason)}'


def create_or_update(cluster, token_name, token_fields):
    """Creates (or updates) the given token on the given cluster"""
    cluster_name = cluster['name']
    cluster_url = cluster['url']

    existing_token_data, existing_token_etag = get_token(cluster, token_name)
    try:
        print_info('Attempting to post on %s cluster...' % terminal.bold(cluster_name))
        json_body = existing_token_data if existing_token_data else {}
        json_body.update(token_fields)
        headers = {'If-Match': existing_token_etag or ''}
        resp = http_util.post(cluster, 'token', json_body, params={'token': token_name}, headers=headers)
        process_post_result(resp)
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
    if len(clusters) > 1:
        default_for_create = [c for c in clusters if c.get('default-for-create', False)]
        num_default_create_clusters = len(default_for_create)
        if num_default_create_clusters == 0:
            raise Exception('You must either specify a cluster via --cluster or set "default-for-create" to true for '
                            'one of your configured clusters.')
        elif num_default_create_clusters > 1:
            raise Exception('You have "default-for-create" set to true for more than one cluster.')
        else:
            cluster = default_for_create[0]
    else:
        cluster = clusters[0]
    token_name = args.pop('token')
    token_fields = args
    return create_or_update(cluster, token_name, token_fields)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    global create_parser
    create_parser = add_parser('create',
                               help='create or update token',
                               description='Create or update a Waiter token. '
                                           'In addition to the optional arguments '
                                           'explicitly listed below, '
                                           'you can optionally provide any Waiter '
                                           'token parameter as a flag. For example, '
                                           'to specify 10 seconds for the '
                                           'grace-period-secs parameter, '
                                           'you can pass --grace-period-secs 10.',
                               aliases=['update'])
    create_parser.add_argument('--name', '-n', help='name of service')
    create_parser.add_argument('--version', '-v', help='version of service')
    create_parser.add_argument('--cmd', '-C', help='command to start service')
    create_parser.add_argument('--cmd-type', '-t', help='command type of service (e.g. "shell")', dest='cmd-type')
    create_parser.add_argument('--cpus', '-c', help='cpus to reserve for service', type=float)
    create_parser.add_argument('--mem', '-m', help='memory to reserve for service', type=int)
    create_parser.add_argument('--ports', help='number of ports to reserve for service', type=int)
    create_parser.add_argument('token', nargs=1)
    return create


def add_implicit_arguments(unknown_args):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    num_unknown_args = len(unknown_args)
    for i in range(num_unknown_args):
        arg = unknown_args[i]
        if arg.startswith(("-", "--")):
            if arg.endswith('-secs') or arg.endswith('-mins') or arg.endswith('-instances'):
                arg_type = int
            elif arg.endswith('-factor'):
                arg_type = float
            elif (i + 1) < num_unknown_args and unknown_args[i + 1].lower() in BOOL_STRINGS:
                arg_type = str2bool
            else:
                arg_type = None
            create_parser.add_argument(arg, dest=arg.lstrip('-'), type=arg_type)
