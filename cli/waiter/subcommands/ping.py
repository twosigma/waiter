
from waiter import http_util, terminal
from waiter.action import ping_service_on_cluster, ping_token_on_cluster
from waiter.querying import print_no_data, query_service, query_token
from waiter.util import check_positive, guard_no_cluster


def ping(clusters, args, _, __):
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
    wait_for_request = args.get('wait', True)
    http_util.set_retries(0)
    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    overall_success = True
    for cluster_name, data in cluster_data_pairs:
        cluster = clusters_by_name[cluster_name]
        if is_service_id:
            success = ping_service_on_cluster(cluster, token_name_or_service_id, timeout, wait_for_request)
        else:
            token_data = data['token']
            token_etag = data['etag']
            token_cluster_name = token_data['cluster'].upper()
            if len(clusters) == 1 or token_explicitly_created_on_cluster(cluster, token_cluster_name):
                success = ping_token_on_cluster(cluster, token_name_or_service_id, timeout,
                                                wait_for_request, token_etag)
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
    default_timeout = 300
    parser = add_parser('ping', help='ping token by name')
    parser.add_argument('token-or-service-id')
    parser.add_argument('--timeout', '-t', help=f'read timeout (in seconds) for ping request (default is '
                                                f'{default_timeout} seconds)',
                        type=check_positive, default=default_timeout)
    parser.add_argument('--service-id', '-s', help='ping by service id instead of token',
                        dest='is-service-id', action='store_true')
    parser.add_argument('--no-wait', '-n', help='do not wait for ping request to return',
                        dest='wait', action='store_false')
    return ping
