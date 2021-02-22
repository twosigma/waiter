import argparse
import logging
import os
from enum import Enum

from waiter import plugins, terminal
from waiter.querying import get_service_id_from_instance_id, print_no_data, print_no_services, query_service, \
    query_services
from waiter.util import get_user_selection, guard_no_cluster, print_info

BASH_PATH = '/bin/bash'


class Destination(Enum):
    TOKEN = 'token'
    SERVICE_ID = 'service_id'
    INSTANCE_ID = 'instance_id'


def get_instances_from_service_id(clusters, service_id, include_active_instances, include_failed_instances,
                                  include_killed_instances):
    query_result = query_service(clusters, service_id)
    num_services = query_result['count']
    if num_services == 0:
        return False
    service = list(query_result['clusters'].values())[0]['service']
    services = []
    if include_active_instances:
        services += service['instances']['active-instances']
    if include_failed_instances:
        services += service['instances']['failed-instances']
    if include_killed_instances:
        services += service['instances']['killed-instances']
    return services


def kubectl_exec_to_instance(kubectl_cmd, api_server, namespace, pod_name, log_directory, command_to_run=None):
    container_name = 'waiter-app'
    args = ['--server', api_server,
            '--namespace', namespace,
            'exec',
            '-it', pod_name,
            '-c', container_name,
            '--',
            '/bin/bash', '-c', f"cd {log_directory}; {' '.join(command_to_run) or 'exec /bin/bash'}"]
    os.execlp(kubectl_cmd, 'kubectl', *args)


def ssh_instance(instance, command_to_run=None):
    print_info(f'Attempting to ssh into instance {terminal.bold(instance["id"])}...')
    log_directory = instance['log-directory']
    k8s_pod_name = instance.get('k8s/pod-name', False)
    if k8s_pod_name:
        k8s_api_server = instance['k8s/api-server-url']
        kubectl_cmd = os.getenv('WAITER_KUBECTL', plugins.get_fn('get-kubectl-cmd', lambda: 'kubectl')())
        k8s_namespace = instance['k8s/namespace']
        print_info(f'Executing ssh to k8s pod {terminal.bold(k8s_pod_name)}')
        logging.debug(f'Executing ssh to k8s pod {terminal.bold(k8s_pod_name)} '
                      f'using namespace={k8s_namespace} api_server={k8s_api_server}')
        kubectl_exec_to_instance(kubectl_cmd, k8s_api_server, k8s_namespace, k8s_pod_name, log_directory, command_to_run)
    else:
        hostname = instance['host']
        command_to_run = command_to_run or [BASH_PATH]
        ssh_cmd = os.getenv('WAITER_SSH', 'ssh')
        args = [ssh_cmd, '-t', hostname, 'cd', log_directory, ';'] + command_to_run
        print_info(f'Executing ssh to {terminal.bold(hostname)}')
        os.execlp(ssh_cmd, *args)


def ssh_instance_id(clusters, instance_id, command):
    service_id = get_service_id_from_instance_id(instance_id)
    instances = get_instances_from_service_id(clusters, service_id, True, True, True)
    if not instances:
        print_no_data(clusters)
        return 1
    found_instance = next((instance
                           for instance in instances
                           if instance['id'] == instance_id),
                          False)
    if not found_instance:
        print_no_data(clusters)
        return 1
    return ssh_instance(found_instance, command)


def ssh_service_id(clusters, service_id, command, include_active_instances, include_failed_instances,
                   include_killed_instances):
    instances = get_instances_from_service_id(clusters, service_id, include_active_instances, include_failed_instances,
                                              include_killed_instances)
    if not instances:
        print_no_data(clusters)
        return 1
    instance_items = [{'instance': instance,
                       'message': instance['id']}
                      for instance in instances]
    select_prompt_message = f'There are multiple instances for service {terminal.bold(service_id)}. ' \
                            f'Select the correct instance:'
    selected_instance_item = get_user_selection(select_prompt_message, instance_items)
    return ssh_instance(selected_instance_item['instance'], command)


def ssh_token(clusters, token, command, include_active_instances, include_failed_instances, include_killed_instances):
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
    return ssh_service_id(clusters, selected_service_item['service']['service-id'], command, include_active_instances,
                          include_failed_instances, include_killed_instances)


def ssh(clusters, args, _, __):
    guard_no_cluster(clusters)
    token_or_service_id_or_instance_id = args.pop('token-or-service-id-or-instance-id')
    command = args.pop('command')
    ssh_destination = args.pop('ssh_destination')
    include_active_instances = args.pop('include_active_instances')
    include_failed_instances = args.pop('include_failed_instances')
    include_killed_instances = args.pop('include_killed_instances')
    if ssh_destination == Destination.TOKEN:
        return ssh_token(clusters, token_or_service_id_or_instance_id, command,
                         include_active_instances, include_failed_instances, include_killed_instances)
    elif ssh_destination == Destination.SERVICE_ID:
        return ssh_service_id(clusters, token_or_service_id_or_instance_id, command,
                              include_active_instances, include_failed_instances, include_killed_instances)
    elif ssh_destination == Destination.INSTANCE_ID:
        return ssh_instance_id(clusters, token_or_service_id_or_instance_id, command)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    # TODO: better help messages describing behavior
    # TODO: docstrings for all functions
    parser = add_parser('ssh',
                        help='ssh to a pod given the token, service-id, or pod name.')
    parser.add_argument('token-or-service-id-or-instance-id')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='ssh_destination', action='store_const', const=Destination.TOKEN,
                          default=Destination.TOKEN)
    id_group.add_argument('--service-id', '-s', dest='ssh_destination', action='store_const',
                          const=Destination.SERVICE_ID)
    id_group.add_argument('--instance-id', '-i', dest='ssh_destination', action='store_const',
                          const=Destination.INSTANCE_ID)
    parser.add_argument('--include-active-instances', dest='include_active_instances', action='store_true', default=True)
    parser.add_argument('--include-failed-instances', dest='include_failed_instances', action='store_true', default=True)
    parser.add_argument('--include-killed-instances', dest='include_killed_instances', action='store_true')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    return ssh
