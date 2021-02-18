import argparse

from waiter import terminal
from waiter.querying import print_no_data, print_no_services, query_service, query_services
from waiter.util import get_user_selection, guard_no_cluster


def ssh_instance(instance):
    print(instance)
    return 0


def ssh_service(clusters, service_id):
    query_result = query_service(clusters, service_id)
    num_services = query_result['count']
    if num_services == 0:
        print_no_data(clusters)
        return 1
    for cluster_name, service_data in query_result['clusters'].items():
        if service_data['count'] > 0:
            service = service_data['service']
            break
    instances = service['instances']['active-instances'] + service['instances']['failed-instances']
    instance_items = [{'instance': instance,
                       'message': instance['id']}
                      for instance in instances]
    select_prompt_message = f'There are multiple instances for service {terminal.bold(service_id)}. ' \
                            f'Select the correct instance:'
    selected_instance_item = get_user_selection(select_prompt_message, instance_items)
    return ssh_instance(selected_instance_item['instance'])


def ssh_token(clusters, token):
    query_result = query_services(clusters, token)
    num_services = query_result['count']
    cluster_data = query_result['clusters']
    if num_services == 0:
        print_no_services(clusters, token)
        return 1
    cluster_items = [{'cluster': cluster,
                      'services': data['services'],
                      'message': cluster}
                     for cluster, data in cluster_data.items()]
    select_prompt_message = f'There are multiple clusters with services for token ' \
                            f'{terminal.bold(token)}. Select the correct cluster:'
    selected_cluster_item = get_user_selection(select_prompt_message, cluster_items)
    services = selected_cluster_item['services']
    service_items = [{'service': service,
                      'message': service['service-id']}
                     for service in services]
    select_prompt_message = f'There are multiple services on cluster ' \
                            f'{terminal.bold(selected_cluster_item["cluster"])}. Select the correct service:'
    selected_service_item = get_user_selection(select_prompt_message, service_items)
    return ssh_service(clusters, selected_service_item['service']['service-id'])


def ssh(clusters, args, _, __):
    guard_no_cluster(clusters)
    token_or_service_id_or_pod_name = args.pop('token-or-service-id-or-pod-name')
    command = args.pop('command')
    is_token = args.pop('is-token')
    is_service_id = args.pop('is-service-id')
    is_pod_name = args.pop('is-pod-name')
    if is_token:
        return ssh_token(clusters, token_or_service_id_or_pod_name)
    elif is_service_id:
        return ssh_service(clusters, token_or_service_id_or_pod_name)
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
