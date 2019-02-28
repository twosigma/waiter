import itertools
import json

from tabulate import tabulate

from waiter.format import format_mem_field
from waiter.querying import query_across_clusters, get_token
from waiter.util import guard_no_cluster


def juxtapose_text(text_a, text_b, buffer_len=15):
    """Places text_a to the left of text_b with a buffer of spaces in between"""
    lines_a = text_a.splitlines()
    lines_b = text_b.splitlines()
    longest_line_length_a = max(map(len, lines_a))
    paired_lines = itertools.zip_longest(lines_a, lines_b, fillvalue="")
    a_columns = longest_line_length_a + buffer_len
    return "\n".join("{0:<{1}}{2}".format(a, a_columns, b) for a, b in paired_lines)


def tabulate_token(cluster_name, token, token_name):
    """Given a token, returns a string containing tables for the fields"""
    left = [['Cluster', cluster_name],
                      ['Owner', token['owner']],
                      ['CPUs', token['cpus']]]
    if token.get('name'):
        left.append(['Name', token['name']])
    if token.get('mem'):
        left.append(['Memory', format_mem_field(token)])
    if token.get('ports'):
        left.append(['Ports Requested', token['ports']])

    right = []

    command = token.get('cmd')
    if command:
        token_command = f'Command:\n{command}'
    else:
        token_command = '<No command specified>'

    if token.get('env') and len(token['env']) > 0:
        environment = '\n\nEnvironment:\n%s' % '\n'.join(['%s=%s' % (k, v) for k, v in token['env'].items()])
    else:
        environment = ''

    left_table = tabulate(left, tablefmt='plain')
    right_table = tabulate(right, tablefmt='plain')
    tables = juxtapose_text(left_table, right_table)
    return f'\n=== Token: {token_name} ===\n\n{tables}\n\n{token_command}{environment}'


def show_data(cluster_name, data, format_fn, token_name):
    """Iterates over the data collection and formats and prints each element"""
    count = len(data)
    if count > 0:
        output = format_fn(cluster_name, data, token_name)
        print(output)
        print()
    return count


def get_token_on_cluster(cluster, token_name):
    """Gets the token with the given name on the given cluster"""
    token = get_token(cluster, token_name)
    if token:
        return {'count': 1, 'token': token}
    else:
        raise Exception(f'Unable to retrieve token information on {cluster["name"]} ({cluster["url"]}).')


def query(clusters, token):
    """
    Uses query_across_clusters to make the token
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(get_token_on_cluster, cluster, token)

    return query_across_clusters(clusters, submit)


def show(clusters, args, _):
    """Prints info for the token with the given token name."""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    token_name = args.get('token')[0]
    query_result = query(clusters, token_name)
    if as_json:
        print(json.dumps(query_result))
    else:
        for cluster_name, entities in query_result['clusters'].items():
            show_data(cluster_name, entities['token'], tabulate_token, token_name)

    if query_result['count'] > 0:
        return 0
    else:
        return 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('show', help='show jobs / instances / groups by uuid')
    show_parser.add_argument('token', nargs=1)
    show_parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    return show
