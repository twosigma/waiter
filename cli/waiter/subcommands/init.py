import getpass
import json
import logging
import os

from waiter import token_post
from waiter.util import deep_merge

parser = None


DEFAULTS = {
    'name': 'your-app-name',
    'metric-group': 'your-metric-group',
    'cmd': 'your command',
    'version': 'your version',
    'cpus': 0.1,
    'mem': 2048,
    'health-check-url': '/your-health-check-endpoint',
    'concurrency-level': 120,
    'permitted-user': '*',
    'run-as-user': getpass.getuser()
}


def init_token_json(_, args, __):
    """Creates (or updates) a Waiter token"""
    logging.debug('args: %s' % args)
    file = os.path.abspath(args.pop('file'))
    should_overwrite = args.pop('force')
    if os.path.isfile(file) and not should_overwrite:
        raise Exception(f'There is already a file at {file}. Use --force if you want to overwrite it.')
    else:
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
    parser.add_argument('--file', '-F', help='name of file to write token JSON to', default='token.json')
    parser.add_argument('--force', '-f', help='overwrite existing file', dest='force', action='store_true')
    return init_token_json


def add_implicit_arguments(unknown_args):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    token_post.add_implicit_arguments(unknown_args, parser)
