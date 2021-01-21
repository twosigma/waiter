import getpass
import logging
import os

from waiter import token_post
from waiter.data_format import determine_format
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


def init_token_json(_, args, __, ___):
    """Creates (or updates) a Waiter token"""
    logging.debug('args: %s' % args)
    file = os.path.abspath(args.pop('file'))
    should_overwrite = args.pop('force')
    if os.path.isfile(file) and not should_overwrite:
        raise Exception(f'There is already a file at {file}. Use --force if you want to overwrite it.')
    else:
        input_format = determine_format(args)
        print(f'Writing token {input_format} to {file}.')

        args.pop('json')
        args.pop('yaml')
        token_fields = deep_merge(DEFAULTS, args)

        with open(file, 'w') as out_file:
            input_format.dump(token_fields, out_file)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    global parser
    action = token_post.Action.INIT
    parser = token_post.register_argument_parser(add_parser, action)
    token_post.add_token_flags(parser)
    parser.add_argument('--file', '-F', help='name of file to write token in JSON (default) or YAML format to',
                        default='token.json')
    parser.add_argument('--force', '-f', help='overwrite existing file', dest='force', action='store_true')
    format_group = parser.add_mutually_exclusive_group()
    format_group.add_argument('--json', help='write the data in JSON format', dest='json', action='store_true')
    format_group.add_argument('--yaml', help='write the data in YAML format', dest='yaml', action='store_true')
    return init_token_json


def add_implicit_arguments(unknown_args):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    token_post.add_implicit_arguments(unknown_args, parser)
