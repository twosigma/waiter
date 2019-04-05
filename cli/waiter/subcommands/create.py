from functools import partial
import argparse
import logging

from waiter import token_post

parser = None


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    global parser
    action = token_post.Action.CREATE
    parser = token_post.register_argument_parser(add_parser, action)
    token_post.add_arguments(parser)
    return partial(token_post.create_or_update_token, action=action)


def boolean_string(s):
    """Converts the given string to a boolean, or raises"""
    b = str2bool(s)
    if b is None:
        raise argparse.ArgumentTypeError('Boolean value expected.')
    else:
        return b


def add_implicit_arguments(unknown_args):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    token_post.add_implicit_arguments(unknown_args, parser)
