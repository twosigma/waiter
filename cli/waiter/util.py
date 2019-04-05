import argparse
import os
import sys
import time
from datetime import datetime, timedelta

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
