import argparse
import logging
from urllib.parse import urlparse

from waiter import configuration, http_util, metrics, version
from waiter.subcommands import create, delete, kill, ping, show, update

parser = argparse.ArgumentParser(description='waiter is the Waiter CLI')
parser.add_argument('--cluster', '-c', help='the name of the Waiter cluster to use')
parser.add_argument('--url', '-u', help='the url of the Waiter cluster to use')
parser.add_argument('--config', '-C', help='the configuration file to use')
parser.add_argument('--verbose', '-v', help='be more verbose/talkative (useful for debugging)',
                    dest='verbose', action='store_true')
parser.add_argument('--version', help='output version information and exit',
                    version=f'%(prog)s version {version.VERSION}', action='version')

subparsers = parser.add_subparsers(dest='action')

actions = {
    'create': create.register(subparsers.add_parser),
    'delete': delete.register(subparsers.add_parser),
    'kill': kill.register(subparsers.add_parser),
    'ping': ping.register(subparsers.add_parser),
    'show': show.register(subparsers.add_parser),
    'update': update.register(subparsers.add_parser)
}


def load_target_clusters(config_map, url=None, cluster=None):
    """Given the config and (optional) url and cluster flags, returns the list of clusters to target"""
    if cluster and url:
        raise Exception('You cannot specify both a cluster name and a cluster url at the same time')

    clusters = None
    config_clusters = config_map.get('clusters')
    if url:
        if urlparse(url).scheme == '':
            url = 'http://%s' % url
        clusters = [{'name': url, 'url': url}]
    elif config_clusters:
        if cluster:
            clusters = [c for c in config_clusters if c.get('name').lower() == cluster.lower()]
            if len(clusters) == 0 and len(config_clusters) > 0:
                config_cluster_names = ', '.join([c.get('name') for c in config_clusters])
                raise Exception(f'You specified cluster {cluster}, which was not present in your config.' +
                                f' You have the following clusters configured: {config_cluster_names}.')
        else:
            clusters = [c for c in config_clusters if 'disabled' not in c or not c['disabled']]

    return clusters


def run(args):
    """
    Main entrypoint to the Waiter CLI. Loads configuration files,
    processes global command line arguments, and calls other command line 
    sub-commands (actions) if necessary.
    """
    args, unknown_args = parser.parse_known_args(args)
    if args.action == 'create':
        create.add_implicit_arguments(unknown_args)
    elif args.action == 'update':
        update.add_implicit_arguments(unknown_args)
    args = parser.parse_args()
    args = vars(args)

    verbose = args.pop('verbose')

    log_format = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
    if verbose:
        logging.getLogger('').handlers = []
        logging.basicConfig(format=log_format, level=logging.DEBUG)
    else:
        logging.disable(logging.FATAL)

    logging.debug('args: %s' % args)

    action = args.pop('action')
    config_path = args.pop('config')
    cluster = args.pop('cluster')
    url = args.pop('url')

    if action is None:
        parser.print_help()
    else:
        config_map = configuration.load_config_with_defaults(config_path)
        try:
            metrics.initialize(config_map)
            metrics.inc(f'command.{action}.runs')
            clusters = load_target_clusters(config_map, url, cluster)
            http_util.configure(config_map)
            args = {k: v for k, v in args.items() if v is not None}
            result = actions[action](clusters, args, config_path)
            logging.debug(f'result: {result}')
            if result == 0:
                metrics.inc(f'command.{action}.result.success')
            else:
                metrics.inc(f'command.{action}.result.failure')
            return result
        finally:
            metrics.close()

    return None
