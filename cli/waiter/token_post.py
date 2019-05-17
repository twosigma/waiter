import json
import logging
import sys
from enum import Enum

import requests

from waiter import terminal, http_util
from waiter.querying import get_token, query_token
from waiter.util import FALSE_STRINGS, print_info, response_message, TRUE_STRINGS, guard_no_cluster, str2bool, \
    load_json_file

BOOL_STRINGS = TRUE_STRINGS + FALSE_STRINGS
INT_PARAM_SUFFIXES = ['-failures', '-index', '-instances', '-length', '-level', '-mins', '-secs']
FLOAT_PARAM_SUFFIXES = ['-factor', '-rate', '-threshold']


class Action(Enum):
    CREATE = 'create'
    UPDATE = 'update'
    INIT = 'init'

    def __str__(self):
        return f'{self.value}'

    def should_patch(self):
        return self is Action.UPDATE


def process_post_result(resp):
    """Prints the result of a token POST"""
    resp_json = resp.json()
    if 'message' in resp_json:
        message = resp_json['message']
        print_info(f'{message}.')
        return

    raise Exception(f'{response_message(resp_json)}')


def post_failed_message(cluster_name, reason):
    """Generates a failed token post message with the given cluster name and reason"""
    return f'Token post {terminal.failed("failed")} on {cluster_name}:\n{terminal.reason(reason)}'


def create_or_update(cluster, token_name, token_fields, action):
    """Creates (or updates) the given token on the given cluster"""
    cluster_name = cluster['name']
    cluster_url = cluster['url']

    existing_token_data, existing_token_etag = get_token(cluster, token_name)
    try:
        print_info(f'Attempting to {action} token on {terminal.bold(cluster_name)}...')
        json_body = existing_token_data if existing_token_data and action.should_patch() else {}
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


def parse_raw_token_spec(r):
    """
    Parse a JSON string containing raw token data.
    Token data must be a dict of token attributes.
    Throws a ValueError if there is a problem parsing the data.
    """
    try:
        content = json.loads(r)
    except Exception:
        raise ValueError('Malformed JSON for raw token.')

    if type(content) is dict:
        return content
    else:
        raise ValueError('Token must be a dictionary of attributes.')


def read_token_from_stdin():
    """Prompts for and then reads token JSON from stdin"""
    print('Enter the raw token JSON (press Ctrl+D on a blank line to submit)', file=sys.stderr)
    token_json = sys.stdin.read()
    return token_json


def create_or_update_token(clusters, args, _, action):
    """Creates (or updates) a Waiter token"""
    guard_no_cluster(clusters)
    logging.debug('args: %s' % args)
    token_name_from_args = args.pop('token', None)
    json_file = args.pop('json', None)
    if json_file:
        if len(args) > 0:
            raise Exception('You cannot specify both a token JSON file and token field flags at the same time.')

        if json_file == '-':
            token_json = read_token_from_stdin()
            token_fields = parse_raw_token_spec(token_json)
        else:
            token_fields = load_json_file(json_file)
            if not token_fields:
                raise Exception(f'Unable to load token JSON from {json_file}.')
    else:
        token_fields = args

    token_name_from_json = token_fields.pop('token', None)
    if token_name_from_args and token_name_from_json:
        raise Exception('You cannot specify the token name both as an argument and in the token JSON file.')

    token_name = token_name_from_args or token_name_from_json
    if not token_name:
        raise Exception('You must specify the token name either as an argument or in a token JSON file via --json.')

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
            query_result = query_token(clusters, token_name)
            if query_result['count'] > 0:
                cluster_names_with_token = list(query_result['clusters'].keys())
                if cluster['name'] not in cluster_names_with_token:
                    cluster = next(c for c in clusters if c['name'] == cluster_names_with_token[0])
                    logging.debug(f'token already exists in: {cluster}')
    else:
        cluster = clusters[0]

    return create_or_update(cluster, token_name, token_fields, action)


def add_arguments(parser):
    """Adds arguments to the given parser"""
    add_token_flags(parser)
    parser.add_argument('--json', help='provide the data in a JSON file', dest='json')
    parser.add_argument('token', nargs='?')


def add_token_flags(parser):
    """Adds the "core" token-field flags to the given parser"""
    parser.add_argument('--name', '-n', help='name of service')
    parser.add_argument('--version', '-v', help='version of service')
    parser.add_argument('--cmd', '-C', help='command to start service')
    parser.add_argument('--cmd-type', '-t', help='command type of service (e.g. "shell")', dest='cmd-type')
    parser.add_argument('--cpus', '-c', help='cpus to reserve for service', type=float)
    parser.add_argument('--mem', '-m', help='memory (in MiB) to reserve for service', type=int)
    parser.add_argument('--ports', help='number of ports to reserve for service', type=int)


def register_argument_parser(add_parser, action):
    """Calls add_parser for the given sub-command (create or update) and returns the parser"""
    sub_command = str(action)
    return add_parser(sub_command,
                      help=f'{sub_command} token',
                      description=f'{sub_command.capitalize()} a Waiter token. '
                      'In addition to the optional arguments '
                      'explicitly listed below, '
                      'you can optionally provide any Waiter '
                      'token parameter as a flag. For example, '
                      'to specify 10 seconds for the '
                      'grace-period-secs parameter, '
                      'you can pass --grace-period-secs 10.')


def possible_int(arg):
    """Attempts to parse arg as an int, returning the string otherwise"""
    try:
        return int(arg)
    except ValueError:
        logging.info(f'failed to parse {arg} as an int, treating it as a string')
        return arg


def possible_float(arg):
    """Attempts to parse arg as a float, returning the string otherwise"""
    try:
        return float(arg)
    except ValueError:
        logging.info(f'failed to parse {arg} as a float, treating it as a string')
        return arg


def add_implicit_arguments(unknown_args, parser):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    num_unknown_args = len(unknown_args)
    for i in range(num_unknown_args):
        arg = unknown_args[i]
        if arg.startswith(("-", "--")):
            if any(arg.endswith(suffix) for suffix in INT_PARAM_SUFFIXES):
                arg_type = possible_int
            elif any(arg.endswith(suffix) for suffix in FLOAT_PARAM_SUFFIXES):
                arg_type = possible_float
            elif (i + 1) < num_unknown_args and unknown_args[i + 1].lower() in BOOL_STRINGS:
                arg_type = str2bool
            else:
                arg_type = None
            parser.add_argument(arg, dest=arg.lstrip('-'), type=arg_type)
