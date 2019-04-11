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
    token_name_or_service_id = args.get('token-or-service-id')
    is_service_id = args.get('is-service-id', False)
    if is_service_id:
        query_result = query_service(clusters, token_name_or_service_id)
    else:
        query_result = query_token(clusters, token_name_or_service_id)

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
        if is_service_id:
            health_check_endpoint = data['service']['effective-parameters']['health-check-url']
            success = ping_service_on_cluster(cluster, token_name_or_service_id, health_check_endpoint, timeout)
        else:
            token_data = data['token']
            token_cluster_name = token_data['cluster'].upper()
            if len(clusters) == 1 or token_explicitly_created_on_cluster(cluster, token_cluster_name):
                # We are hard-coding the default health-check-url here to /status; this could
                # alternatively be retrieved from /settings, but we want to skip the extra request
                health_check_endpoint = token_data.get('health-check-url', '/status')
                success = ping_token_on_cluster(cluster, token_name_or_service_id, health_check_endpoint, timeout)
            else:
                print(f'Not pinging token {terminal.bold(token_name_or_service_id)} '
                      f'in {terminal.bold(cluster_name)} '
                      f'because it was created in {terminal.bold(token_cluster_name)}.')
                success = True
        overall_success = overall_success and success
    return 0 if overall_success else 1


def token_explicitly_created_on_cluster(cluster, token_cluster_name):
    """Returns true if the given token cluster matches the configured cluster name of the given cluster"""
    cluster_settings, _ = http_util.make_data_request(cluster, lambda: http_util.get(cluster, '/settings'))
    cluster_config_name = cluster_settings['cluster-config']['name'].upper()
    created_on_this_cluster = token_cluster_name == cluster_config_name
    return created_on_this_cluster


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ping', help='ping token by name')
    parser.add_argument('token-or-service-id')
    parser.add_argument('--timeout', '-t', help='read timeout (in seconds) for ping request',
                        type=check_positive, default=60)
    parser.add_argument('--service-id', '-s', help='ping by service id instead of token',
                        dest='is-service-id', action='store_true')
    return ping
