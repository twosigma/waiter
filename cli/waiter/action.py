import json
import logging
import requests
import socket
import ssl
import time
from urllib.parse import urljoin

from tabulate import tabulate

from waiter import http_util, terminal
from waiter.format import format_last_request_time
from waiter.format import format_status
from waiter.querying import get_service, get_service_id_from_instance_id, get_services_using_token
from waiter.querying import print_no_data, query_service, query_services, query_token
from waiter.util import get_in, is_service_current, str2bool, response_message, print_error, wait_until


_second_to_millis = 1000


class CustomPingResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def print_ping_error(response_json):
    ping_error = ''

    try:
        response_error = response_message(response_json, default_message='')
        ping_error = f'{response_error} '

        ping_response_body = get_in(response_json, ['ping-response', 'body']) or "{}"
        ping_response_json = json.loads(ping_response_body)
        ping_response_error = response_message(ping_response_json, default_message='')
        ping_error = f'{ping_error} {ping_response_error}'
    except json.JSONDecodeError:
        logging.debug('Ping response is not in json format, cannot display waiter-error message.')

    if ping_error:
        print_error(ping_error.strip())
    return None


def make_ping_request(cluster, timeout, wait_for_request, token_name, expected_parameters):

    correlation_id = f'waiter-ping-{time.time()}'

    def make_ping_request_helper(queue_timeout_ms, timeout_ms, read_timeout_secs):
        request_headers = {
            'X-CID': correlation_id,
            'X-Waiter-Queue-Timeout': str(queue_timeout_ms),
            'X-Waiter-Token': token_name,
            'X-Waiter-Timeout': str(timeout_ms)
        }
        ping_response = http_util.get(cluster, '/waiter-ping', headers=request_headers, read_timeout=read_timeout_secs)
        logging.debug(f'Response status code: {ping_response.status_code}')
        return ping_response

    expected_parameters_empty = len(expected_parameters) == 0
    timeout_seconds = timeout if wait_for_request else 5
    timeout_millis = timeout_seconds * 1000
    if expected_parameters_empty:
        default_queue_timeout_millis = 300000
        queue_timeout_millis = max(default_queue_timeout_millis, timeout_millis)
        read_timeout = timeout_seconds if wait_for_request else (timeout_seconds + 5)
        response = make_ping_request_helper(queue_timeout_millis, timeout_millis, read_timeout)
        return response
    else:
        iter_timeout_secs = 2
        iter_timeout_millis = iter_timeout_secs * 1000
        iter_queue_timeout_millis = iter_timeout_secs * 1000
        iter_read_timeout = iter_timeout_secs if wait_for_request else (iter_timeout_secs + 5)
        logging.debug(f'iter_timeout_millis: {iter_timeout_millis}, '
                      f'iter_queue_timeout_millis: {iter_queue_timeout_millis}, '
                      f'iter_read_timeout: {iter_read_timeout}')

        result_response = None
        start_time = time.time()
        while True:
            iter_start_time = time.time()
            try:
                response = make_ping_request_helper(iter_queue_timeout_millis, iter_timeout_millis, iter_read_timeout)
                response_json = response.json()
                service_description = get_in(response_json, ['service-description'])
                all_params_match = True
                for param_name, param_value in expected_parameters.items():
                    actual_value = get_in(service_description, [param_name])
                    if param_value != actual_value:
                        logging.debug(f'{param_name} has value of "{actual_value}", require it to be "{param_value}"')
                        all_params_match = False
                if all_params_match:
                    logging.debug(f'Service description reports expected parameters: {expected_parameters}')
                    result_response = response
                    break
            except Exception as ex:
                logging.debug(f'Error in pinging {token_name}: {ex}')
            iter_end_time = time.time()

            total_elapsed_millis = (iter_end_time - start_time) * 1000
            if total_elapsed_millis >= timeout_millis:
                logging.debug(f'Ping timed out while waiting for expected parameters')
                break

            iter_elapsed_millis = (iter_end_time - iter_start_time) * 1000
            if iter_elapsed_millis < iter_timeout_millis:
                iter_sleep_secs = (iter_timeout_millis - iter_elapsed_millis) / 1000.0
                logging.debug(f'Sleeping for {iter_sleep_secs} seconds before retrying ping')
                time.sleep(iter_sleep_secs)

        if result_response is None:
            json_data = {'waiter-error': {'message': f'Unable to ping service with parameters {expected_parameters}'}}
            result_response = CustomPingResponse(json_data, 400)

        return result_response


def ping_on_cluster(cluster, timeout, wait_for_request, token_name, expected_parameters, service_exists_fn):
    """Pings using the given token name (or ^SERVICE-ID#) in the given cluster."""
    cluster_name = cluster['name']

    def perform_ping():
        status = None
        try:
            response = make_ping_request(cluster, timeout, wait_for_request, token_name, expected_parameters)
            response_json = response.json()
            if response.status_code == 200:
                status = response_json.get('service-state', {}).get('status')
                ping_response = response_json['ping-response']
                ping_response_result = ping_response['result']
                if ping_response_result in ['descriptor-error', 'received-response']:
                    ping_response_status = ping_response['status']
                    if ping_response_status == 200:
                        print(terminal.success('Ping successful.'))
                        result = True
                    else:
                        print_error(f'Ping responded with non-200 status {ping_response_status}.')
                        print_ping_error(response_json)
                        result = False
                elif ping_response_result == 'timed-out':
                    if wait_for_request:
                        print_error('Ping request timed out.')
                        print_ping_error(response_json)
                        result = False
                    else:
                        logging.debug('ignoring ping request timeout due to --no-wait')
                        result = True
                else:
                    print_error(f'Encountered unknown ping result: {ping_response_result}.')
                    print_ping_error(response_json)
                    result = False
            else:
                print_ping_error(response_json)
                result = False
        except Exception as ex:
            result = False
            message = f'Encountered error while pinging in {cluster_name}: {ex}.'
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


def check_ssl(token_name, port, timeout_seconds):
    """Returns true when a socket to the token's DNS name doesn't raise an SSL or connection error"""
    timeout_millis = timeout_seconds * 1000
    context = ssl.create_default_context()
    with socket.create_connection((token_name, port)) as cleartext_socket:
        with context.wrap_socket(cleartext_socket, server_hostname=token_name) as tls_socket:
            tls_version = tls_socket.version()
            print(f'Successfully connected to {token_name}:{port} using {tls_version}')
    return True


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


def ping_token_on_cluster(cluster, token_name, expected_parameters, timeout, wait_for_request,
                          current_token_etag):
    """Pings the token with the given token name in the given cluster."""
    cluster_name = cluster['name']
    print(f'Pinging token {terminal.bold(token_name)} '
          f'in {terminal.bold(cluster_name)}...')
    return ping_on_cluster(cluster, timeout, wait_for_request, token_name, expected_parameters,
                           lambda: token_has_current_service(cluster, token_name, current_token_etag))


def service_is_active(cluster, service_id):
    """If the given service id is active, returns the service, else None"""
    service = get_service(cluster, service_id)
    return service if (service and service.get('status') != 'Inactive') else None


def ping_service_on_cluster(cluster, service_id, expected_parameters, timeout, wait_for_request):
    """Pings the service with the given service id in the given cluster."""
    cluster_name = cluster['name']
    print(f'Pinging service {terminal.bold(service_id)} '
          f'in {terminal.bold(cluster_name)}...')
    return ping_on_cluster(cluster, timeout, wait_for_request, f'^SERVICE-ID#{service_id}', expected_parameters,
                           lambda: service_is_active(cluster, service_id))


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


def token_explicitly_created_on_cluster(cluster, token_cluster_name):
    """Returns true if the given token cluster matches the configured cluster name of the given cluster
    or if the token cluster matches the configured cluster name in the .waiter.json file"""
    cluster_settings, _ = http_util.make_data_request(cluster, lambda: http_util.get(cluster, '/settings'))
    cluster_config_name = cluster_settings['cluster-config']['name'].upper()
    cluster_local_config_name = cluster['name'].upper()

    # we consider the token having the same cluster value as the configured cluster in .waiter.json as an alias
    # which makes it easier to change a Waiter cluster name without making breaking changes to the CLI client.
    created_on_this_cluster = token_cluster_name in [cluster_config_name, cluster_local_config_name]
    return created_on_this_cluster


def process_ping_request(clusters, token_name_or_service_id, is_service_id, expected_parameters,
                         timeout_secs, wait_for_request):
    """Pings the service using the given token name or service-id.
    Returns False if the ping request fails or times out.
    Returns True if the ping request completes successfully."""
    if is_service_id:
        query_result = query_service(clusters, token_name_or_service_id)
    else:
        query_result = query_token(clusters, token_name_or_service_id)

    if query_result['count'] == 0:
        print_no_data(clusters)
        return False

    http_util.set_retries(0)
    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    overall_success = True
    for cluster_name, data in cluster_data_pairs:
        cluster = clusters_by_name[cluster_name]
        if is_service_id:
            success = ping_service_on_cluster(cluster, token_name_or_service_id, expected_parameters,
                                              timeout_secs, wait_for_request)
        else:
            token_data = data['token']
            token_etag = data['etag']
            token_cluster_name = token_data['cluster'].upper()
            if len(clusters) == 1 or token_explicitly_created_on_cluster(cluster, token_cluster_name):
                success = ping_token_on_cluster(cluster, token_name_or_service_id, expected_parameters,
                                                timeout_secs, wait_for_request, token_etag)
            else:
                print(f'Not pinging token {terminal.bold(token_name_or_service_id)} '
                      f'in {terminal.bold(cluster_name)} '
                      f'because it was created in {terminal.bold(token_cluster_name)}.')
                success = True
        overall_success = overall_success and success
    return overall_success


def send_signal_to_instance_on_cluster(cluster, service_id, instance_id, signal_type, query_params):
    """Send sigkill request to the specific instance"""
    cluster_name = cluster['name']
    http_util.set_retries(0)
    try:
        print(
            f'Sending {terminal.bold(signal_type)} request to instance {terminal.bold(instance_id)} in {terminal.bold(cluster_name)}...')
        resp = http_util.post(cluster, f'/apps/{service_id}/instance/{instance_id}/{signal_type}', '',
                              params=query_params)
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            resp_json = resp.json()
            logging.debug(f'Response json: {resp_json}')
            signal_response = resp_json.get("signal-response", {})
            success = signal_response.get('success')
            if success:
                print(f'Successfully sent {signal_type} to {instance_id} in {cluster_name}.')
                return True
            else:
                message = signal_response.get('message')
                print(f'Unable to send {signal_type} to {instance_id} in {cluster_name}. {message}')
                return False
        else:
            print_error(response_message(resp.json()))
            return False
    except requests.exceptions.ReadTimeout:
        message = f'Request timed out while signaling {instance_id} in {cluster_name}.'
        logging.exception(message)
        print_error(message)
        return False
    except Exception:
        message = f'Encountered error while signaling {instance_id} in {cluster_name}.'
        logging.exception(message)
        print_error(message)
        return False


def process_signal_request(clusters, instance_id, signal_type, query_params, no_instance_result=False):
    """Sends signal to the instance given the instance id.
    Returns False if there are no instances or if there is no active instance with the same instance id
    Returns True if the signal request is sent successfully."""
    service_id = get_service_id_from_instance_id(instance_id)
    query_result = query_service(clusters, service_id)
    num_services = query_result['count']
    if num_services == 0:
        print_no_data(clusters)
        return no_instance_result

    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    for cluster_name, data in cluster_data_pairs:
        service = data['service']
        if service['status'] == 'Inactive':
            logging.debug(f"Skipping {service['status']} service {service_id} on {cluster_name} cluster")
            continue

        active_instances = service.get('instances', {}).get('active-instances')
        for instance in active_instances:
            if instance['id'] == instance_id:
                cluster = clusters_by_name[cluster_name]
                return send_signal_to_instance_on_cluster(cluster, service_id, instance_id, signal_type, query_params)

    print(f'No active instance with ID {terminal.bold(instance_id)}')
    return False
