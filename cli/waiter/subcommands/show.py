import json
import logging

from tabulate import tabulate

from waiter.format import format_mem_field, format_timestamp_string, format_field_name
from waiter.querying import query_across_clusters, get_token, print_no_data
from waiter.util import guard_no_cluster


def tabulate_token(cluster_name, token, token_name):
    """Given a token, returns a string containing tables for the fields"""
    table = [['Cluster', cluster_name],
             ['Owner', token['owner']]]
    if token.get('name'):
        table.append(['Name', token['name']])
    if token.get('cpus'):
        table.append(['CPUs', token['cpus']])
    if token.get('mem'):
        table.append(['Memory', format_mem_field(token)])
    if token.get('ports'):
        table.append(['Ports Requested', token['ports']])

    explicit_keys = ('cmd', 'cpus', 'last-update-time', 'last-update-user', 'mem', 'name', 'owner', 'ports')
    ignored_keys = ('cluster', 'previous', 'root')
    for key, value in token.items():
        if key not in (explicit_keys + ignored_keys):
            table.append([format_field_name(key), value])

    command = token.get('cmd')
    if command:
        token_command = f'Command:\n{command}'
    else:
        token_command = '<No command specified>'

    if token.get('env') and len(token['env']) > 0:
        environment = '\n\nEnvironment:\n%s' % '\n'.join(['%s=%s' % (k, v) for k, v in token['env'].items()])
    else:
        environment = ''

    table_text = tabulate(table, tablefmt='plain')
    last_update_time = format_timestamp_string(token['last-update-time'])
    last_update_user = token['last-update-user']
    return f'\n' \
           f'=== Token: {token_name} ===\n' \
           f'\n' \
           f'Last Updated: {last_update_time} ({last_update_user})\n' \
           f'\n' \
           f'{table_text}\n' \
           f'\n' \
           f'{token_command}' \
           f'{environment}'


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
    token_data, _ = get_token(cluster, token_name, include='metadata')
    if token_data:
        return {'count': 1, 'token': token_data}
    else:
        logging.info(f'Unable to retrieve token information on {cluster["name"]} ({cluster["url"]}).')
        return {'count': 0}


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
        for cluster_name, entities in sorted(query_result['clusters'].items()):
            show_data(cluster_name, entities['token'], tabulate_token, token_name)

    if query_result['count'] > 0:
        return 0
    else:
        if not as_json:
            print_no_data(clusters)
        return 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('show', help='show token by name')
    show_parser.add_argument('token', nargs=1)
    show_parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    return show
