import argparse
import collections
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from tabulate import tabulate

from waiter import terminal

TRUE_STRINGS = ('yes', 'true', 'y')
FALSE_STRINGS = ('no', 'false', 'n')


def deep_merge(a, b):
    """Merges a and b, letting b win if there is a conflict"""
    merged = a.copy()
    for key in b:
        b_value = b[key]
        merged[key] = b_value
        if key in a:
            a_value = a[key]
            if isinstance(a_value, dict) and isinstance(b_value, dict):
                merged[key] = deep_merge(a_value, b_value)
    return merged


def current_user():
    """Returns the value of the USER environment variable"""
    return os.environ['USER']


def print_error(text):
    """Prints text to stderr, colored as a failure"""
    print(terminal.failed(text), file=sys.stderr)


def print_info(text, end='\n'):
    """Prints text to stdout"""
    print(text, flush=True, end=end)


def guard_no_cluster(clusters):
    """Throws if no clusters have been specified, either via configuration or via the command line"""
    if not clusters:
        raise Exception('You must specify at least one cluster.')


def str2bool(v):
    """Converts the given string to a boolean, or returns None"""
    if v.lower() in TRUE_STRINGS:
        return True
    elif v.lower() in FALSE_STRINGS:
        return False
    else:
        return None


def response_message(resp_json):
    """Pulls the error message out of a Waiter response"""
    if 'waiter-error' in resp_json and 'message' in resp_json['waiter-error']:
        message = resp_json['waiter-error']['message']
        if not message.endswith('.'):
            message = f'{message}.'
    else:
        message = 'Encountered unexpected error.'
    return message


def wait_until(pred, timeout=30, interval=5):
    """
    Wait, retrying a predicate until it is True, or the
    timeout value has been exceeded.
    """
    if timeout:
        finish = datetime.now() + timedelta(seconds=timeout)
    else:
        finish = None

    while True:
        result = pred()

        if result:
            break

        if finish and datetime.now() >= finish:
            break

        time.sleep(interval)

    return result


def check_positive(value):
    """Checks that the given value is a positive integer"""
    try:
        integer = int(value)
    except:
        raise argparse.ArgumentTypeError(f'{value} is not an integer')
    if integer <= 0:
        raise argparse.ArgumentTypeError(f'{value} is not a positive integer')
    return integer


def load_json_file(path):
    """Decode a JSON formatted file."""
    content = None

    if os.path.isfile(path):
        with open(path) as json_file:
            try:
                logging.debug(f'attempting to load json from {path}')
                content = json.load(json_file)
            except Exception:
                logging.exception(f'encountered exception when loading json from {path}')
    else:
        logging.info(f'{path} is not a file')

    return content


def is_service_current(service, current_token_etag, token_name):
    """Returns True if any of the given service's source tokens is the current token"""
    is_current = any(source['version'] == current_token_etag and source['token'] == token_name
                     for sources in service['source-tokens']
                     for source in sources)
    return is_current


def is_admin_enabled():
    """Returns True if current user is an admin"""
    return str2bool(os.getenv('WAITER_ADMIN', default=FALSE_STRINGS[0]))


def get_user_selection(select_message, column_names, items, short_circuit_choice=True):
    """
    :param select_message: list of local cluster configs from the configuration file
    :param column_names: column names of the tabular list of choices. Order is maintained. The first field should be
    unique among the list of items.
    :param items: List of items with fields associated with the column names for display.
    :param short_circuit_choice: When True and only one item in items, return that item as the selection without user
    prompt.
    :exception Raises exception when user input is invalid
    :return selected item (an element from the items list)
    """
    if short_circuit_choice and len(items) == 0: #
        return items[0]
    rows = [collections.OrderedDict([('INDEX', idx)] + list(map(lambda column_name:
                                                                (column_name.upper(), str(item[column_name]).lower()),
                                                                column_names)))
            for idx, item in enumerate(items)]
    alternate_id_field = column_names[0]
    items_table = tabulate(rows, headers='keys', tablefmt='plain')
    print(items_table)
    # for index, item in enumerate(items):
    #     info_str = ''
    #     for name in column_names[1:]:
    #         info_str += f'{name}={item[name]} '
    #     print(f'{index}. {terminal.bold(item[column_names[0]])} ({info_str.strip()})')
    answer = input(f'Enter the Index or {alternate_id_field} of your choice: ')
    try:
        selected_item = next((item
                              for item in items
                              if item[alternate_id_field].upper() == answer.strip().upper()),
                             False)
        if selected_item:
            return selected_item
        index = int(answer)
        if index < 0 or index >= len(items):
            raise Exception('Input is out of range!')
        return items[int(answer)]
    except ValueError as error:
        print_error('Input received did not match any of the choices!')
        raise error
