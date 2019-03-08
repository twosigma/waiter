import os
import sys

from waiter import terminal


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
