import json
import logging
import os

from waiter import token_post
from waiter.util import deep_merge

parser = None


DEFAULTS = {
    'cmd': 'your command goes here',
    'version': 'your version goes here',
    'cpus': 0.1,
    'mem': 2048
}


def init_token_json(_, args, __):
    """Creates (or updates) a Waiter token"""
    logging.debug('args: %s' % args)
    file = os.path.abspath(args.pop('file'))
    print(f'Writing token JSON to {file}.')
    token_fields = deep_merge(DEFAULTS, args)
    with open(file, 'w') as outfile:
        json.dump(token_fields, outfile, indent=2)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    global parser
    action = token_post.Action.INIT
    parser = token_post.register_argument_parser(add_parser, action)
    token_post.add_token_flags(parser)
    parser.add_argument('--file', '-f', help='name of file to write token JSON to', default='token.json')
    return init_token_json


def add_implicit_arguments(unknown_args):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    token_post.add_implicit_arguments(unknown_args, parser)
