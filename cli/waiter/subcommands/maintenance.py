from functools import partial

import requests

from waiter import terminal, http_util
from waiter.token_post import process_post_result, post_failed_message
from waiter.querying import get_cluster_with_token, get_token
from waiter.util import guard_no_cluster, logging, print_info


def check_maintenance(clusters, args):
    guard_no_cluster(clusters)
    token_name = args['token']
    cluster = get_cluster_with_token(clusters, token_name)
    existing_token_data, existing_token_etag = get_token(cluster, token_name)
    maintenance_mode_active = 'maintenance' in existing_token_data
    print_info(f'{token_name} is {"" if maintenance_mode_active else "not "}in maintenance mode')
    return 0 if maintenance_mode_active else 1


def start_maintenance(clusters, args):
    guard_no_cluster(clusters)
    token_name = args['token']
    cluster = get_cluster_with_token(clusters, token_name)
    cluster_name = cluster['name']
    cluster_url = cluster['url']
    existing_token_data, existing_token_etag = get_token(cluster, token_name)
    json_body = existing_token_data
    headers = {'If-Match': existing_token_etag}
    params = {'token': token_name}
    try:
        update_doc = {"maintenance": {"message": args['message']}}
        json_body.update(update_doc)
        resp = http_util.post(cluster, 'token', json_body, params=params, headers=headers)
        process_post_result(resp)
        return 0
    except requests.exceptions.ReadTimeout as rt:
        logging.exception(rt)
        print_info(terminal.failed(
            f'Encountered read timeout with {cluster_name} ({cluster_url}). The operation may have completed.'))
        return 1
    except IOError as ioe:
        logging.exception(ioe)
        reason = f'Cannot connect to {cluster_name} ({cluster_url})'
        message = post_failed_message(cluster_name, reason)
        print_info(f'{message}\n')


def stop_maintenance(clusters, args):
    guard_no_cluster(clusters)
    token_name = args['token']
    cluster = get_cluster_with_token(clusters, token_name)
    cluster_name = cluster['name']
    cluster_url = cluster['url']
    existing_token_data, existing_token_etag = get_token(cluster, token_name)
    maintenance_mode_active = 'maintenance' in existing_token_data
    json_body = existing_token_data
    headers = {'If-Match': existing_token_etag}
    params = {'token': token_name}
    try:
        if maintenance_mode_active:
            json_body.pop("maintenance")
        else:
            raise Exception("Token is not in maintenance mode")
        resp = http_util.post(cluster, 'token', json_body, params=params, headers=headers)
        process_post_result(resp)
        return 0
    except requests.exceptions.ReadTimeout as rt:
        logging.exception(rt)
        print_info(terminal.failed(
            f'Encountered read timeout with {cluster_name} ({cluster_url}). The operation may have completed.'))
        return 1
    except IOError as ioe:
        logging.exception(ioe)
        reason = f'Cannot connect to {cluster_name} ({cluster_url})'
        message = post_failed_message(cluster_name, reason)
        print_info(f'{message}\n')


def maintenance(parser, clusters, args, _):
    logging.debug('args: %s' % args)
    sub_func = args.get('sub_func', None)
    if sub_func is None:
        parser.print_help()
        return 0
    else:
        return sub_func(clusters, args)


def register_action(add_parser, sub_action, sub_func):
    parser = add_parser(sub_action, help=f'{sub_action} maintenance mode for a token')
    parser.add_argument('token')
    parser.set_defaults(sub_func=sub_func)
    return parser


def register_check(add_parser):
    register_action(add_parser, 'check', check_maintenance)


def register_stop(add_parser):
    register_action(add_parser, 'stop', stop_maintenance)


def register_start(add_parser):
    start_parser = register_action(add_parser, 'start', start_maintenance)
    start_parser.add_argument('message',
                              help='Your message will be provided in a 503 response for requests to the token. '
                                   'The message cannot be more than 512 characters.')


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('maintenance',
                        help='manage maintenance mode for a token',
                        description='Manage maintenance mode for a Waiter token.')
    subparsers = parser.add_subparsers()
    register_check(subparsers.add_parser)
    register_start(subparsers.add_parser)
    register_stop(subparsers.add_parser)
    return partial(maintenance, parser)
