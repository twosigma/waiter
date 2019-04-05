import logging

from waiter import http_util, terminal
from waiter.querying import query_token, print_no_data, query_service
from waiter.util import guard_no_cluster, response_message, print_error, check_positive


def ping_on_cluster(cluster, health_check_endpoint, timeout, token_name):
    """Pings using the given token name (or ^SERVICE-ID#) in the given cluster."""
    cluster_name = cluster['name']
    try:
        headers = {
            'X-Waiter-Token': token_name,
            'X-Waiter-Fallback-Period-Secs': '0'
        }
        resp = http_util.get(cluster, health_check_endpoint, headers=headers, read_timeout=timeout)
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            print(terminal.success(f'Ping successful.'))
            print(resp.text)
            return True
        else:
            print_error(response_message(resp.json()))
            return False
    except Exception:
        message = f'Encountered error while pinging in {cluster_name}.'
        logging.exception(message)
        print_error(message)


def ping_token_on_cluster(cluster, token_name, health_check_endpoint, timeout):
    """Pings the token with the given token name in the given cluster."""
    cluster_name = cluster['name']
    print(f'Pinging token {terminal.bold(token_name)} '
          f'at {terminal.bold(health_check_endpoint)} '
          f'in {terminal.bold(cluster_name)}...')
    return ping_on_cluster(cluster, health_check_endpoint, timeout, token_name)


def ping_service_on_cluster(cluster, service_id, health_check_endpoint, timeout):
    """Pings the service with the given service id in the given cluster."""
    cluster_name = cluster['name']
    print(f'Pinging service {terminal.bold(service_id)} '
          f'at {terminal.bold(health_check_endpoint)} '
          f'in {terminal.bold(cluster_name)}...')
    return ping_on_cluster(cluster, health_check_endpoint, timeout, f'^SERVICE-ID#{service_id}')


def ping(clusters, args, _):
    """Pings the token with the given token name."""
    guard_no_cluster(clusters)
    token_name = args.get('token')
    service_id = args.get('service-id', None)

    if token_name and service_id:
        raise Exception('You cannot provide both a token name and a service id.')

    if token_name:
        query_result = query_token(clusters, token_name)
    elif service_id:
        query_result = query_service(clusters, service_id)
    else:
        raise Exception('You must provide either a token name or service id (via --service-id).')

    if query_result['count'] == 0:
        print_no_data(clusters)
        return 1

    timeout = args.get('timeout', None)
    http_util.set_retries(0)
    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    overall_success = True
    for cluster_name, data in cluster_data_pairs:
        cluster = clusters_by_name[cluster_name]
        if service_id:
            health_check_endpoint = data['service']['effective-parameters'].get('health-check-url', '/status')
            success = ping_service_on_cluster(cluster, service_id, health_check_endpoint, timeout)
        else:
            health_check_endpoint = data['token'].get('health-check-url', '/status')
            success = ping_token_on_cluster(cluster, token_name, health_check_endpoint, timeout)
        overall_success = overall_success and success
    return 0 if overall_success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ping', help='ping token by name')
    parser.add_argument('token', nargs='?')
    parser.add_argument('--timeout', '-t', help='read timeout (in seconds) for ping request',
                        type=check_positive, default=60)
    parser.add_argument('--service-id', '-s', help='service id for ping request', dest='service-id')
    return ping
