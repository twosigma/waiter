import logging

from tabulate import tabulate

from waiter import http_util, terminal
from waiter.format import format_timestamp_string
from waiter.querying import query_services, services_using_token
from waiter.util import guard_no_cluster, str2bool, response_message, print_error, wait_until


def kill_service_on_cluster(cluster, service_id, token_name):
    """Kills the service with the given service id in the given cluster."""

    def service_is_killed():
        return service_id not in [s['service-id'] for s in services_using_token(cluster, token_name)]

    cluster_name = cluster['name']
    try:
        print(f'Killing service {terminal.bold(service_id)} in {terminal.bold(cluster_name)}...')
        resp = http_util.delete(cluster, f'/apps/{service_id}')
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            killed = wait_until(service_is_killed, timeout=30, interval=1)
            if killed:
                print(f'Successfully killed {service_id} in {cluster_name}.')
                return True
            else:
                print_error('Timeout waiting for service to die.')
                return False
        else:
            print_error(response_message(resp.json()))
            return False
    except Exception:
        message = f'Encountered error while killing {service_id} in {cluster_name}.'
        logging.exception(message)
        print_error(message)


def format_status(status):
    """Formats service status"""
    if status == 'Running':
        return terminal.running(status)
    else:
        return status


def kill(clusters, args, _):
    """Kills the service(s) using the given token name."""
    guard_no_cluster(clusters)
    token_name = args.get('token')[0]
    query_result = query_services(clusters, token_name)
    num_services = query_result['count']
    if num_services == 0:
        clusters_text = ' / '.join([terminal.bold(c['name']) for c in clusters])
        print(f'There are no services using token {terminal.bold(token_name)} in {clusters_text}.')
        return 1
    elif num_services > 1:
        print(f'There are {num_services} services using token {terminal.bold(token_name)}:')

    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    overall_success = True
    for cluster_name, data in cluster_data_pairs:
        for service in data['services']:
            service_id = service['service-id']
            if args.get('force', False) or num_services == 1:
                should_kill = True
            else:
                status = format_status(service['status'])
                healthy_count = service['instance-counts']['healthy-instances']
                unhealthy_count = service['instance-counts']['unhealthy-instances']
                last_request_time = format_timestamp_string(service['last-request-time'])
                url = service['url']
                table = [['Status', status],
                         ['Healthy', healthy_count],
                         ['Unhealthy', unhealthy_count],
                         ['URL', url]]
                table_text = tabulate(table, tablefmt='plain')
                print(f'\n'
                      f'=== {terminal.bold(cluster_name)} / {terminal.bold(service_id)} ===\n'
                      f'\n'
                      f'Last Request: {last_request_time}\n'
                      f'\n'
                      f'{table_text}\n')
                answer = input(f'Kill service in {terminal.bold(cluster_name)}? ')
                should_kill = str2bool(answer)
            if should_kill:
                cluster = clusters_by_name[cluster_name]
                success = kill_service_on_cluster(cluster, service_id, token_name)
                overall_success = overall_success and success
    return 0 if overall_success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='kill services')
    parser.add_argument('token', nargs=1)
    parser.add_argument('--force', '-f', help='kill all services, never prompt', dest='force', action='store_true')
    return kill
