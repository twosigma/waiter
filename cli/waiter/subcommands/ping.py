import logging

from waiter import http_util, terminal
from waiter.querying import query_token, print_no_data
from waiter.util import guard_no_cluster, response_message, print_error


def ping_token_on_cluster(cluster, token_name, health_check_endpoint):
    """Pings the token with the given token name in the given cluster."""
    cluster_name = cluster['name']
    try:
        print(f'Pinging token {terminal.bold(token_name)} '
              f'at {terminal.bold(health_check_endpoint)} '
              f'in {terminal.bold(cluster_name)}...')
        headers = {
            'X-Waiter-Token': token_name,
            'X-Waiter-Fallback-Period-Secs': '0'
        }
        resp = http_util.get(cluster, health_check_endpoint, headers=headers)
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            print(terminal.success(f'Successfully pinged {token_name} in {cluster_name}.'))
            print(resp.text)
            return True
        else:
            print_error(response_message(resp.json()))
            return False
    except Exception as e:
        message = f'Encountered error while pinging token {token_name} in {cluster_name}.'
        logging.error(e, message)
        print_error(message)


def ping(clusters, args, _):
    """Pings the token with the given token name."""
    guard_no_cluster(clusters)
    token_name = args.get('token')[0]
    query_result = query_token(clusters, token_name)
    if query_result['count'] == 0:
        print_no_data(clusters)
        return 1

    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    overall_success = True
    for cluster_name, data in cluster_data_pairs:
        cluster = clusters_by_name[cluster_name]
        success = ping_token_on_cluster(cluster, token_name, data['token'].get('health-check-url', '/status'))
        overall_success = overall_success and success
    return 0 if overall_success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ping', help='ping token by name')
    parser.add_argument('token', nargs=1)
    return ping
