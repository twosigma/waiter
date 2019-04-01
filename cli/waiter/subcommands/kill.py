import logging

from waiter import http_util, terminal
from waiter.querying import print_no_data, query_services
from waiter.util import guard_no_cluster, str2bool, response_message, print_error


def kill_service_on_cluster(cluster, service_id):
    """Kills the service with the given service id in the given cluster."""
    cluster_name = cluster['name']
    try:
        print(f'Killing service {terminal.bold(service_id)} in {terminal.bold(cluster_name)}...')
        resp = http_util.delete(cluster, f'/apps/{service_id}')
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            print(f'Successfully killed {service_id} in {cluster_name}.')
            return True
        else:
            print_error(response_message(resp.json()))
            return False
    except Exception as e:
        message = f'Encountered error while killing {service_id} in {cluster_name}.'
        logging.error(e, message)
        print_error(message)


def kill(clusters, args, _):
    """Kills the service(s) using the given token name."""
    guard_no_cluster(clusters)
    token_name = args.get('token')[0]
    query_result = query_services(clusters, token_name)
    num_services = query_result['count']
    if num_services == 0:
        print_no_data(clusters)
        return 1
    elif num_services > 1:
        print(f'There are {num_services} services using token {terminal.bold(token_name)}.')

    cluster_data_pairs = sorted(query_result['clusters'].items())
    clusters_by_name = {c['name']: c for c in clusters}
    overall_success = True
    for cluster_name, data in cluster_data_pairs:
        for service in data['services']:
            service_id = service["service-id"]
            if args.get('force', False) or num_services == 1:
                should_kill = True
            else:
                answer = input(f'Kill service {terminal.bold(service_id)} in {terminal.bold(cluster_name)}? ')
                should_kill = str2bool(answer)
            if should_kill:
                cluster = clusters_by_name[cluster_name]
                success = kill_service_on_cluster(cluster, service_id)
                overall_success = overall_success and success
    return 0 if overall_success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='kill services')
    parser.add_argument('token', nargs=1)
    parser.add_argument('--force', '-f', help='kill all services, never prompt', dest='force', action='store_true')
    return kill
