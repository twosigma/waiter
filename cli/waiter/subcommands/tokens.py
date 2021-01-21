import collections
import getpass

from tabulate import tabulate

from waiter.data_format import display_data
from waiter.format import format_timestamp_string
from waiter.querying import print_no_data, query_tokens
from waiter.util import guard_no_cluster


def query_result_to_cluster_token_pairs(query_result):
    """Given a query result structure, returns a sequence of (cluster, token) pairs from the result"""
    cluster_token_pairs = ((c, t) for c, e in query_result['clusters'].items() for t in e['tokens'])
    cluster_token_pairs_sorted = sorted(cluster_token_pairs, key=lambda p: (p[1]['token'], p[0]))
    return cluster_token_pairs_sorted


def print_as_table(query_result):
    """Given a collection of (cluster, token) pairs, formats a table showing the most relevant token fields"""
    cluster_token_pairs = query_result_to_cluster_token_pairs(query_result)
    rows = [collections.OrderedDict([("Cluster", cluster),
                                     ("Owner", token['owner']),
                                     ("Token", token['token']),
                                     ("Maintenance", token.get('maintenance', False)),
                                     ("Updated", format_timestamp_string(token['last-update-time']))])
            for (cluster, token) in cluster_token_pairs]
    token_table = tabulate(rows, headers='keys', tablefmt='plain')
    print(token_table)


def tokens(clusters, args, _, __):
    """Prints info for the tokens owned by the given user."""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    as_yaml = args.get('yaml')
    user = args.get('user')

    query_result = query_tokens(clusters, user)
    if as_json or as_yaml:
        display_data(args, query_result)
    else:
        print_as_table(query_result)

    if query_result['count'] > 0:
        return 0
    else:
        if not as_json and not as_yaml:
            print_no_data(clusters)
        return 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('tokens', help='list tokens by owner')
    parser.add_argument('--user', '-u', help='list tokens owned by a user', default=getpass.getuser())
    format_group = parser.add_mutually_exclusive_group()
    format_group.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    format_group.add_argument('--yaml', help='show the data in YAML format', dest='yaml', action='store_true')
    return tokens
