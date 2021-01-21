import logging
import json

from waiter import http_util, terminal
from waiter.format import format_status
from waiter.querying import get_service, get_services_using_token, print_no_data, query_service, query_token
from waiter.util import check_positive, guard_no_cluster, is_service_current, print_error, response_message, wait_until


def ping_on_cluster(cluster, timeout, wait_for_request, token_name, service_exists_fn):
    """Pings using the given token name (or ^SERVICE-ID#) in the given cluster."""
    cluster_name = cluster['name']

    def perform_ping():
        status = None
        try:
            timeout_seconds = timeout if wait_for_request else 5
            headers = {
                'X-Waiter-Token': token_name,
                'X-Waiter-Timeout': str(timeout_seconds * 1000)
            }
            read_timeout = timeout_seconds if wait_for_request else (timeout_seconds + 5)
            resp = http_util.get(cluster, '/waiter-ping', headers=headers, read_timeout=read_timeout)
            logging.debug(f'Response status code: {resp.status_code}')
            resp_json = resp.json()
            if resp.status_code == 200:
                status = resp_json['service-state']['status']
                ping_response = resp_json['ping-response']
                ping_response_result = ping_response['result']
                if ping_response_result == 'received-response':
                    ping_response_status = ping_response['status']
                    if ping_response_status == 200:
                        print(terminal.success('Ping successful.'))
                        result = True
                    else:
                        print_error(f'Ping responded with non-200 status {ping_response_status}.')
                        try:
                            ping_response_waiter_error = json.loads(ping_response['body'])['waiter-error']['message']
                            print_error(ping_response_waiter_error)
                        except json.JSONDecodeError:
                            logging.debug('Ping response is not in json format, cannot display waiter-error message.')
                        except KeyError:
                            logging.debug('Ping response body does not contain waiter-error message.')
                        result = False
                elif ping_response_result == 'timed-out':
                    if wait_for_request:
                        print_error('Ping request timed out.')
                        result = False
                    else:
                        logging.debug('ignoring ping request timeout due to --no-wait')
                        result = True
                else:
                    print_error(f'Encountered unknown ping result: {ping_response_result}.')
                    result = False
            else:
                print_error(response_message(resp_json))
                result = False
        except Exception:
            result = False
            message = f'Encountered error while pinging in {cluster_name}.'
            logging.exception(message)
            if wait_for_request:
                print_error(message)

        return result, status

    def check_service_status():
        result = wait_until(service_exists_fn, timeout=timeout, interval=5)
        if result:
            print(f'Service is currently {format_status(result["status"])}.')
            return True
        else:
            print_error('Timeout while waiting for service to start.')
            return False

    succeeded, service_status = perform_ping()
    if succeeded:
        if service_status is None or service_status == 'Inactive':
            check_service_status()
        else:
            print(f'Service is currently {format_status(service_status)}.')
    return succeeded


def token_has_current_service(cluster, token_name, current_token_etag):
    """If the given token has a "current" service, returns that service else None"""
    services = get_services_using_token(cluster, token_name)
    if services is not None:
        services = [s for s in services if is_service_current(s, current_token_etag, token_name)]
        return services[0] if len(services) > 0 else False
    else:
        cluster_name = cluster['name']
        print_error(f'Unable to retrieve services using token {token_name} in {cluster_name}.')
        return None


def ping_token_on_cluster(cluster, token_name, timeout, wait_for_request,
                          current_token_etag):
    """Pings the token with the given token name in the given cluster."""
    cluster_name = cluster['name']
    print(f'Pinging token {terminal.bold(token_name)} '
          f'in {terminal.bold(cluster_name)}...')
    return ping_on_cluster(cluster, timeout, wait_for_request,
                           token_name, lambda: token_has_current_service(cluster, token_name, current_token_etag))


def service_is_active(cluster, service_id):
    """If the given service id is active, returns the service, else None"""
    service = get_service(cluster, service_id)
    return service if (service and service.get('status') != 'Inactive') else None


def ping_service_on_cluster(cluster, service_id, timeout, wait_for_request):
    """Pings the service with the given service id in the given cluster."""
    cluster_name = cluster['name']
    print(f'Pinging service {terminal.bold(service_id)} '
          f'in {terminal.bold(cluster_name)}...')
    return ping_on_cluster(cluster, timeout, wait_for_request,
                           f'^SERVICE-ID#{service_id}', lambda: service_is_active(cluster, service_id))


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
    parser = add_parser('ping', help='ping token by name')
    parser.add_argument('token-or-service-id')
    parser.add_argument('--timeout', '-t', help='read timeout (in seconds) for ping request',
                        type=check_positive, default=300)
    parser.add_argument('--service-id', '-s', help='ping by service id instead of token',
                        dest='is-service-id', action='store_true')
    parser.add_argument('--no-wait', '-n', help='do not wait for ping request to return',
                        dest='wait', action='store_false')
    return ping
