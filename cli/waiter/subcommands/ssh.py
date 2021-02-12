import argparse

from waiter import terminal
from waiter.querying import print_no_services, query_services
from waiter.util import guard_no_cluster, print_error


def get_user_selection(select_message, items):
    print(f'{terminal.bold(select_message)}')
    for count, item in enumerate(items):
        print(f'{terminal.bold(f"[{count}].")} {item["message"]}')
    answer = input('Enter the number associated with your choice: ')
    print('\n')
    try:
        index = int(answer)
        if index < 0 or index >= len(items):
            raise Exception('Input is out of range!')
        return items[int(answer)]
    except ValueError as error:
        print_error('Input received was not an integer!')
        raise error


def ssh(clusters, args, _, __):
    guard_no_cluster(clusters)
    token_or_service_id_or_pod_name = args.pop('token-or-service-id-or-pod-name')
    command = args.pop('command')
    is_token = args.pop('is-token')
    is_service_id = args.pop('is-service-id')
    is_pod_name = args.pop('is-pod-name')
    if is_token:
        query_result = query_services(clusters, token_or_service_id_or_pod_name)
        num_services = query_result['count']
        cluster_data = query_result['clusters']
        if num_services == 0:
            print_no_services(clusters, token_or_service_id_or_pod_name)
            return 1
        cluster_items = [{'cluster': cluster,
                          'services': data['services'],
                          'message': cluster}
                         for cluster, data in cluster_data.items()]
        if len(cluster_data.keys()) > 0:
            select_prompt_message = f'There are multiple clusters with services for token ' \
                                    f'{terminal.bold(token_or_service_id_or_pod_name)}. Select the correct cluster:'
            selected_cluster_item = get_user_selection(select_prompt_message, cluster_items)
        else:
            selected_cluster_item = cluster_items[0]
        services = selected_cluster_item['services']
        service_items = [{'service': service,
                          'message': service['service-id']}
                         for service in services]
        if len(services) > 0:
            select_prompt_message = f'There are multiple services on cluster ' \
                                    f'{terminal.bold(selected_cluster_item["cluster"])}. Select the correct service:'
            selected_service_item = get_user_selection(select_prompt_message, service_items)
            return 1
        else:
            selected_service_item = service_items[0]
        # get selection for which service they want

    elif is_service_id:
        # get all pods for the service id
        return 0
    elif is_pod_name:
        return 0

    return 0


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh',
                        help='ssh to a pod given the token, service-id, or pod name. Only kubernetes is supported.')
    parser.add_argument('token-or-service-id-or-pod-name')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='is-token', action='store_true', default=True)
    id_group.add_argument('--service-id', '-s', dest='is-service-id', action='store_true')
    id_group.add_argument('--pod-name', '-p', dest='is-pod-name', action='store_true')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    return ssh
