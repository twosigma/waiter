import argparse
from functools import partial

from waiter import token_post

parser = None


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    global parser
    action = token_post.Action.UPDATE
    parser = token_post.register_argument_parser(add_parser, action)
    token_post.add_arguments(parser)
    deep_merge_group = parser.add_mutually_exclusive_group(required=False)
    deep_merge_group.add_argument('--deep-merge', action='store_true', dest='deep-merge',
                                  help='Deep merge updates into existing token configuration. '
                                       'This is useful if you want to add environment variables or metadata without '
                                       'overriding the entire object.')
    deep_merge_group.add_argument('--shallow-merge', action='store_false', dest='deep-merge', help=argparse.SUPPRESS)
    return partial(token_post.create_or_update_token, action=action)


def add_implicit_arguments(unknown_args):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    token_post.add_implicit_arguments(unknown_args, parser)
