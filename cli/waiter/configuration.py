import json
import logging
import os
import sys

from waiter.util import deep_merge, load_json_file

# Base locations to check for configuration files, relative to the executable.
# Always tries to load these in.
BASE_CONFIG_PATHS = ['.waiter.json',
                     '../.waiter.json',
                     '../config/.waiter.json']

# Additional locations to check for configuration files if one isn't given on the command line
ADDITIONAL_CONFIG_PATHS = ['.waiter.json',
                           os.path.expanduser('~/.waiter.json')]

DEFAULT_CONFIG = {'http': {'retries': 2,
                           'connect-timeout': 3.05,
                           'read-timeout': 20},
                  'metrics': {'disabled': True,
                              'max-retries': 2,
                              'timeout': 0.15}}


def __load_first_json_file(paths):
    """Returns the contents of the first parseable JSON file in a list of paths."""
    if paths is None:
        paths = []
    contents = ((os.path.abspath(p), load_json_file(os.path.abspath(p))) for p in paths if p)
    return next(((p, c) for p, c in contents if c), (None, None))


def __load_base_config():
    """Loads the base configuration map."""
    base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    paths = [os.path.join(base_dir, path) for path in BASE_CONFIG_PATHS]
    _, config = __load_first_json_file(paths)
    return config


def __load_local_config(config_path):
    """
    Loads the configuration map, using the provided config_path if not None,
    otherwise, searching the additional config paths for a valid JSON config file
    """
    if config_path:
        if os.path.isfile(config_path):
            with open(config_path) as json_file:
                config = json.load(json_file)
        else:
            raise Exception(f'The configuration path specified ({config_path}) is not valid.')
    else:
        config_path, config = __load_first_json_file(ADDITIONAL_CONFIG_PATHS)

    return config_path, config


def load_config_with_defaults(config_path=None):
    """Loads the configuration map to use, merging in the defaults"""
    base_config = __load_base_config()
    base_config = base_config or {}
    base_config = deep_merge(DEFAULT_CONFIG, base_config)
    config_path, config = __load_local_config(config_path)
    config = config or {}
    config = deep_merge(base_config, config)
    logging.debug(f'using configuration: {config}')
    return config
