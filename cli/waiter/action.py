import json
import logging
import requests
from urllib.parse import urljoin

from tabulate import tabulate

from waiter import http_util, terminal
from waiter.format import format_last_request_time
from waiter.format import format_status
from waiter.querying import get_service, get_services_using_token
from waiter.querying import print_no_data, query_service, query_services
from waiter.util import is_service_current, str2bool, response_message, print_error, wait_until


def ping_on_cluster(cluster, timeout, wait_for_request, token_name, service_exists_fn):
    """Pings using the given token name (or ^SERVICE-ID#) in the given cluster."""
    cluster_name = cluster['name']

    def perform_ping():
        status = None
        try:
            default_queue_timeout_millis = 300000
            timeout_seconds = timeout if wait_for_request else 5
            timeout_millis = timeout_seconds * 1000
            headers = {
                'X-Waiter-Queue-Timeout': str(max(default_queue_timeout_millis, timeout_millis)),
                'X-Waiter-Token': token_name,
                'X-Waiter-Timeout': str(timeout_millis)
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


def kill_service_on_cluster(cluster, service_id, timeout_seconds):
    """Kills the service with the given service id in the given cluster."""
    cluster_name = cluster['name']
    http_util.set_retries(0)
    try:
        print(f'Killing service {terminal.bold(service_id)} in {terminal.bold(cluster_name)}...')
        params = {'timeout': timeout_seconds * 1000}
        resp = http_util.delete(cluster, f'/apps/{service_id}', params=params, read_timeout=timeout_seconds)
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            routers_agree = resp.json().get('routers-agree')
            if routers_agree:
                print(f'Successfully killed {service_id} in {cluster_name}.')
                return True
            else:
                print(f'Successfully killed {service_id} in {cluster_name}. '
                      f'Server-side timeout waiting for routers to update.')
                return False
        else:
            print_error(response_message(resp.json()))
            return False
    except requests.exceptions.ReadTimeout:
        message = f'Request timed out while killing {service_id} in {cluster_name}.'
        logging.exception(message)
        print_error(message)
    except Exception:
        message = f'Encountered error while killing {service_id} in {cluster_name}.'
        logging.exception(message)
        print_error(message)


def process_kill_request(clusters, token_name_or_service_id, is_service_id, force_flag, timeout_secs,
                         no_service_result=False):
    """Kills the service(s) using the given token name or service-id.
    Returns False if no services can be found or if there was a failure in deleting any service.
    Returns True if all services using the token were deleted successfully."""
    if is_service_id:
        query_result = query_service(clusters, token_name_or_service_id)
        num_services = query_result['count']
        if num_services == 0:
            print_no_data(clusters)
            return no_service_result
    else:
        query_result = query_services(clusters, token_name_or_service_id)
        num_services = query_result['count']
        if num_services == 0:
            clusters_text = ' / '.join([terminal.bold(c['name']) for c in clusters])
            print(f'There are no services using token {terminal.bold(token_name_or_service_id)} in {clusters_text}.')
            return no_service_result
        elif num_services > 1:
            print(f'There are {num_services} services using token {terminal.bold(token_name_or_service_id)}:')

    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    overall_success = True
    for cluster_name, data in cluster_data_pairs:
        if is_service_id:
            service = data['service']
            service['service-id'] = token_name_or_service_id
            services = [service]
        else:
            services = sorted(data['services'], key=lambda s: s.get('last-request-time', None) or '', reverse=True)

        for service in services:
            service_id = service['service-id']
            cluster = clusters_by_name[cluster_name]
            status_string = service['status']
            status = format_status(status_string)
            inactive = status_string == 'Inactive'
            should_kill = False
            if force_flag or num_services == 1:
                should_kill = True
            else:
                url = urljoin(cluster['url'], f'apps/{service_id}')
                if is_service_id:
                    active_instances = service['instances']['active-instances']
                    healthy_count = len([i for i in active_instances if i['healthy?']])
                    unhealthy_count = len([i for i in active_instances if not i['healthy?']])
                else:
                    healthy_count = service['instance-counts']['healthy-instances']
                    unhealthy_count = service['instance-counts']['unhealthy-instances']

                last_request_time = format_last_request_time(service)
                run_as_user = service['service-description']['run-as-user']
                table = [['Status', status],
                         ['Healthy', healthy_count],
                         ['Unhealthy', unhealthy_count],
                         ['URL', url],
                         ['Run as user', run_as_user]]
                table_text = tabulate(table, tablefmt='plain')
                print(f'\n'
                      f'=== {terminal.bold(cluster_name)} / {terminal.bold(service_id)} ===\n'
                      f'\n'
                      f'Last Request: {last_request_time}\n'
                      f'\n'
                      f'{table_text}\n')
                if not inactive:
                    answer = input(f'Kill service in {terminal.bold(cluster_name)}? ')
                    should_kill = str2bool(answer)

            if inactive:
                print(f'Service {terminal.bold(service_id)} on {terminal.bold(cluster_name)} cannot be killed because '
                      f'it is already {status}.')
                should_kill = False

            if should_kill:
                success = kill_service_on_cluster(cluster, service_id, timeout_secs)
                overall_success = overall_success and success
    return overall_success
