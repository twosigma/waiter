import argparse
import logging
import os
from enum import Enum

from waiter import plugins, terminal
from waiter.format import format_boolean, format_status, format_timestamp_string
from waiter.querying import get_service_id_from_instance_id, get_target_cluster_from_token, get_token, print_no_data, \
    print_no_services, query_service, query_services, get_services_on_cluster
from waiter.util import get_user_selection, guard_no_cluster, is_admin_enabled, print_info, is_service_current

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
    instances = []
    if include_active_instances:
        instances += service['instances']['active-instances']
    if include_failed_instances:
        instances += service['instances']['failed-instances']
    if include_killed_instances:
        instances += service['instances']['killed-instances']
    return instances


def kubectl_exec_to_instance(kubectl_cmd, api_server, namespace, pod_name, container_name, log_directory,
                             command_to_run=None):
    args = ['--server', api_server,
            '--namespace', namespace,
            'exec',
            '-it', pod_name,
            '-c', container_name,
            '--',
            '/bin/bash', '-c', f"cd {log_directory}; {' '.join(command_to_run) or 'exec /bin/bash'}"]
    os.execlp(kubectl_cmd, 'kubectl', *args)


def ssh_instance(instance, container_name, command_to_run=None):
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
        kubectl_exec_to_instance(kubectl_cmd, k8s_api_server, k8s_namespace, k8s_pod_name, container_name,
                                 log_directory, command_to_run)
    else:
        hostname = instance['host']
        command_to_run = command_to_run or [BASH_PATH]
        ssh_cmd = os.getenv('WAITER_SSH', 'ssh')
        args = [ssh_cmd, '-t', hostname, 'cd', log_directory, ';'] + command_to_run
        print_info(f'Executing ssh to {terminal.bold(hostname)}')
        os.execlp(ssh_cmd, *args)


def ssh_instance_id(clusters, instance_id, command, container_name):
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
    return ssh_instance(found_instance, container_name, command)


def ssh_service_id(clusters, service_id, command, container_name, skip_prompts, include_active_instances,
                   include_failed_instances, include_killed_instances):
    instances = get_instances_from_service_id(clusters, service_id, include_active_instances, include_failed_instances,
                                              include_killed_instances)
    if not instances or len(instances) == 0:
        print_no_data(clusters)
        return 1
    instance_column_names = ['Instance ID', 'Host', 'Healthy?']
    instance_items = [{'instance': instance,
                       'Instance ID': instance['id'],
                       'Host': instance['host'],
                       'Healthy?': format_boolean(instance['healthy?'])}
                      for instance in instances]
    if skip_prompts:
        selected_instance_item = instance_items[0]
    else:
        select_prompt_message = f'There are multiple instances for service {terminal.bold(service_id)}. ' \
                                f'Select the correct instance:'
        selected_instance_item = get_user_selection(select_prompt_message, instance_column_names, instance_items)
    return ssh_instance(selected_instance_item['instance'], container_name, command)


def ssh_token(clusters, enforce_cluster, token, command, container_name, skip_prompts, include_active_instances,
              include_failed_instances, include_killed_instances):
    if skip_prompts:
        cluster = get_target_cluster_from_token(clusters, token, enforce_cluster)
        _, token_etag = get_token(cluster, token)
        query_result = get_services_on_cluster(cluster, token)
        if query_result['count'] == 0:
            print_no_services(clusters, token)
            return 1
        services = query_result['services']
        selected_service_id = next((s['service-id']
                                   for s in services if is_service_current(s, token_etag, token)),
                                   False)
        if not selected_service_id:
            print_no_data(clusters)
            return 1
    else:
        query_result = query_services(clusters, token)
        num_services = query_result['count']
        cluster_data = query_result['clusters']
        if num_services == 0:
            print_no_services(clusters, token)
            return 1
        cluster_column_names = ['Cluster', 'Services', 'Instances', 'In-flight Requests', 'Last Request Time']
        cluster_items = [{'Cluster': cluster,
                          'Services': len(data['services']),
                          'Instances': sum(service['instance-counts']['healthy-instances'] +
                                           service['instance-counts']['unhealthy-instances']
                                           for service in data['services']),
                          'In-flight Requests': sum(service['request-metrics']['outstanding']
                                                    for service in data['services']),
                          'Last Request Time': format_timestamp_string(max(service['last-request-time']
                                                                           for service in data['services'])),
                          'services': data['services']}
                         for cluster, data in cluster_data.items()
                         if len(data['services']) > 0]
        select_prompt_message = f'There are multiple clusters with services for token ' \
                                f'{terminal.bold(token)}. Select the correct cluster:'
        selected_cluster_item = get_user_selection(select_prompt_message, cluster_column_names, cluster_items)
        services = selected_cluster_item['services']
        service_column_names = ['Service ID', 'Status', 'Instances', 'In-flight Requests', 'Last Request Time']
        service_items = [{'service': service,
                          'Service ID': service['service-id'],
                          'Status': format_status(service['status']),
                          'Instances': service['instance-counts']['healthy-instances'] +
                                       service['instance-counts']['unhealthy-instances'],
                          'In-flight Requests': service['request-metrics']['outstanding'],
                          'Last Request Time': format_timestamp_string(service['last-request-time'])}
                         for service in services]
        select_prompt_message = f'There are multiple services on cluster ' \
                                f'{terminal.bold(selected_cluster_item["Cluster"])}. Select the correct service:'
        selected_service_item = get_user_selection(select_prompt_message, service_column_names, service_items)
        selected_service_id = selected_service_item['service']['service-id']
    return ssh_service_id(clusters, selected_service_id, command, container_name, skip_prompts,
                          include_active_instances, include_failed_instances, include_killed_instances)


def ssh(clusters, args, _, enforce_cluster):
    guard_no_cluster(clusters)
    token_or_service_id_or_instance_id = args.pop('token-or-service-id-or-instance-id')
    command = args.pop('command')
    ssh_destination = args.pop('ssh_destination')
    include_active_instances = args.pop('include_active_instances')
    include_failed_instances = args.pop('include_failed_instances')
    include_killed_instances = args.pop('include_killed_instances')
    container_name = args.pop('container-name', 'waiter-app')
    skip_prompts = args.pop('quick')
    if ssh_destination == Destination.TOKEN:
        return ssh_token(clusters, enforce_cluster, token_or_service_id_or_instance_id, command, container_name,
                         skip_prompts, include_active_instances, include_failed_instances, include_killed_instances)
    elif ssh_destination == Destination.SERVICE_ID:
        return ssh_service_id(clusters, token_or_service_id_or_instance_id, command, container_name, skip_prompts,
                              include_active_instances, include_failed_instances, include_killed_instances)
    elif ssh_destination == Destination.INSTANCE_ID:
        return ssh_instance_id(clusters, token_or_service_id_or_instance_id, command, container_name)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh',
                        help='ssh to a Waiter instance',
                        description='ssh to an instance given the token, service id, or instance id. Working directory '
                                    'will be the log directory.')
    parser.add_argument('token-or-service-id-or-instance-id')
    if is_admin_enabled():
        parser.add_argument('--container-name', help='specify the container name you want to ssh into. Defaults to '
                                                     '"waiter-app". Has no effect if instance is not k8s pod.')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='ssh_destination', action='store_const', const=Destination.TOKEN,
                          default=Destination.TOKEN, help='Default; ssh with token')
    id_group.add_argument('--service-id', '-s', dest='ssh_destination', action='store_const',
                          const=Destination.SERVICE_ID, help='ssh using a service id')
    id_group.add_argument('--instance-id', '-i', dest='ssh_destination', action='store_const',
                          const=Destination.INSTANCE_ID, help='ssh directly to instance id')
    parser.add_argument('--quick', '-q', dest='quick', action='store_true',
                        help='Skips cluster prompt by selecting the one that the token is configured to, services '
                             'prompt by selecting the service that the token currently refers to, and instances prompt '
                             'by selecting a random one. Has no effect if an instance-id is provided.')
    parser.add_argument('--include-active-instances', dest='include_active_instances', action='store_true',
                        default=True,
                        help='included by default; includes active instances for possible ssh destination')
    parser.add_argument('--include-failed-instances', dest='include_failed_instances', action='store_true',
                        default=True,
                        help='included by default; includes failed instances for possible ssh destination')
    parser.add_argument('--include-killed-instances', dest='include_killed_instances', action='store_true',
                        help='includes killed instances for possible ssh destination')
    parser.add_argument('command', nargs=argparse.REMAINDER, help='command to be run on instance')
    return ssh
