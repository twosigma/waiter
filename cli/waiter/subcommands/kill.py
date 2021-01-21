import logging
import requests
from urllib.parse import urljoin

from tabulate import tabulate

from waiter import http_util, terminal
from waiter.format import format_last_request_time, format_status
from waiter.querying import print_no_data, query_service, query_services
from waiter.util import guard_no_cluster, str2bool, response_message, print_error, wait_until, check_positive


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


def kill(clusters, args, _, __):
    """Kills the service(s) using the given token name."""
    guard_no_cluster(clusters)
    token_name_or_service_id = args.get('token-or-service-id')
    is_service_id = args.get('is-service-id', False)
    if is_service_id:
        query_result = query_service(clusters, token_name_or_service_id)
        num_services = query_result['count']
        if num_services == 0:
            print_no_data(clusters)
            return 1
    else:
        query_result = query_services(clusters, token_name_or_service_id)
        num_services = query_result['count']
        if num_services == 0:
            clusters_text = ' / '.join([terminal.bold(c['name']) for c in clusters])
            print(f'There are no services using token {terminal.bold(token_name_or_service_id)} in {clusters_text}.')
            return 1
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
            if args.get('force', False) or num_services == 1:
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
                success = kill_service_on_cluster(cluster, service_id, args['timeout'])
                overall_success = overall_success and success
    return 0 if overall_success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='kill services')
    parser.add_argument('token-or-service-id')
    parser.add_argument('--force', '-f', help='kill all services, never prompt', dest='force', action='store_true')
    parser.add_argument('--service-id', '-s', help='kill by service id instead of token',
                        dest='is-service-id', action='store_true')
    parser.add_argument('--timeout', '-t', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=30)
    return kill
