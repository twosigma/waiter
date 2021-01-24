from functools import partial

import requests

from waiter import terminal, http_util
from waiter.token_post import process_post_result, post_failed_message
from waiter.querying import get_target_cluster_from_token, get_token
from waiter.util import guard_no_cluster, logging, print_info


def _is_token_in_maintenance_mode(token_data):
    return 'maintenance' in token_data


def _get_existing_token_data(clusters, token_name, enforce_cluster):
    guard_no_cluster(clusters)
    cluster = get_target_cluster_from_token(clusters, token_name, enforce_cluster)
    existing_token_data, existing_token_etag = get_token(cluster, token_name)
    return cluster, existing_token_data, existing_token_etag


def _update_token(cluster, token_name, existing_token_etag, body):
    cluster_name = cluster['name']
    cluster_url = cluster['url']
    headers = {'If-Match': existing_token_etag}
    params = {'token': token_name}
    try:
        resp = http_util.post(cluster, 'token', body, params=params, headers=headers)
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


def check_maintenance(clusters, args, enforce_cluster):
    """Checks if a token is in maintenance mode and displays the result. Returns 0 if the token is in maintenance mode
    and returns 1 if the token is NOT in maintenance mode."""
    token_name = args['token']
    _, existing_token_data, existing_token_etag = _get_existing_token_data(clusters, token_name, enforce_cluster)
    maintenance_mode_active = _is_token_in_maintenance_mode(existing_token_data)
    print_info(f'{token_name} is {"" if maintenance_mode_active else "not "}in maintenance mode')
    return 0 if maintenance_mode_active else 1


def start_maintenance(clusters, args, enforce_cluster):
    """Sets the token in maintenance mode by updating the token user metadata fields"""
    token_name = args['token']
    cluster, existing_token_data, existing_token_etag = _get_existing_token_data(clusters, token_name, enforce_cluster)
    json_body = existing_token_data
    update_doc = {"maintenance": {"message": args['message']}}
    json_body.update(update_doc)
    return _update_token(cluster, token_name, existing_token_etag, json_body)


def stop_maintenance(clusters, args, enforce_cluster):
    """Stops maintenance mode for a token by deleting the 'maintenance' user metadata field in the token data"""
    token_name = args['token']
    cluster, existing_token_data, existing_token_etag = _get_existing_token_data(clusters, token_name, enforce_cluster)
    maintenance_mode_active = _is_token_in_maintenance_mode(existing_token_data)
    if not maintenance_mode_active:
        raise Exception("Token is not in maintenance mode")
    json_body = existing_token_data
    json_body.pop("maintenance")
    return _update_token(cluster, token_name, existing_token_etag, json_body)


def maintenance(parser, clusters, args, _, enforce_cluster):
    """Calls the sub action for maintenance command. If no sub action is provided then displays the help message."""
    logging.debug('args: %s' % args)
    sub_func = args.get('sub_func', None)
    if sub_func is None:
        parser.print_help()
        return 0
    else:
        return sub_func(clusters, args, enforce_cluster)


def register_check(add_parser):
    """Registers the maintenance check parser"""
    parser = add_parser('check',
                        help='Checks if a token is in maintenance mode. Exits with code 0 if the token is in '
                             'maintenance mode and 1 if the token is not in maintenance mode')
    parser.add_argument('token')
    parser.set_defaults(sub_func=check_maintenance)


def register_stop(add_parser):
    """Registers the maintenance stop parser"""
    parser = add_parser('stop', help='Stop maintenance mode for a token. Requests to the token will be handled '
                                     'normally.')
    parser.add_argument('token')
    parser.set_defaults(sub_func=stop_maintenance)


def register_start(add_parser):
    """Registers the maintenance start parser"""
    parser = add_parser('start',
                        help='Start maintenance mode for a token. All requests to this token will begin to receive a '
                             '503 response.')
    parser.add_argument('token')
    parser.add_argument('message',
                        help='Your message will be provided in a 503 response for requests to the token. '
                             'The message cannot be more than 512 characters.')
    parser.set_defaults(sub_func=start_maintenance)


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
