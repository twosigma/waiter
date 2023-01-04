import argparse
import logging
import os

from waiter import plugins, terminal
from waiter.instance_select import Destination, get_instance_id_from_destination
from waiter.querying import get_service_id_from_instance_id, print_no_data, query_service
from waiter.util import guard_no_cluster, print_info

BASH_PATH = '/bin/bash'


def map_instances_with_status(instances, status):
    return [{'_status': status, **inst} for inst in instances]


def get_instances_from_service_id(clusters, service_id, include_active_instances, include_failed_instances,
                                  include_killed_instances):
    query_result = query_service(clusters, service_id)
    num_services = query_result['count']
    if num_services == 0:
        return False
    services = map(lambda service_data: service_data['service'], query_result['clusters'].values())
    instances = []
    for service in services:
        if include_active_instances:
            instances += map_instances_with_status(service['instances']['active-instances'], 'active')
        if include_failed_instances:
            instances += map_instances_with_status(service['instances']['failed-instances'], 'failed')
        if include_killed_instances:
            instances += map_instances_with_status(service['instances']['killed-instances'], 'killed')
    return instances


def kubectl_exec_to_instance(kubectl_cmd, namespace, pod_name, container_name, log_directory,
                             command_to_run=None, api_server=None, context=None):
    if context is not None:
        args = ['--context', context]
    elif api_server is not None:
        args = ['--server', api_server]
    args = [*args,
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
        context = instance.get('k8s/context')
        k8s_api_server = instance['k8s/api-server-url']
        kubectl_cmd = os.getenv('WAITER_KUBECTL', plugins.get_fn('get-kubectl-cmd', lambda: 'kubectl')())
        k8s_namespace = instance['k8s/namespace']
        print_info(f'Executing ssh to k8s pod {terminal.bold(k8s_pod_name)}')
        logging.debug(f'Executing ssh to k8s pod {terminal.bold(k8s_pod_name)} '
                      f'using namespace={k8s_namespace} api_server={k8s_api_server}')
        kubectl_exec_to_instance(kubectl_cmd, k8s_namespace, k8s_pod_name, container_name,
                                 log_directory, command_to_run, api_server=k8s_api_server, context=context)
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
    if instances is False:
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


def ssh(clusters, args, _, enforce_cluster):
    guard_no_cluster(clusters)
    token_or_service_id_or_instance_id = args.pop('token-or-service-id-or-instance-id')
    command = args.pop('command')
    ssh_destination = args.pop('ssh_destination')
    include_active_instances = args.pop('include_active_instances')
    include_failed_instances = args.pop('include_failed_instances')
    include_killed_instances = args.pop('include_killed_instances')
    container_name = args.pop('container_name', 'waiter-app')
    skip_prompts = args.pop('quick')

    instance_id = get_instance_id_from_destination(clusters, enforce_cluster, token_or_service_id_or_instance_id,
                                                   ssh_destination, skip_prompts, include_active_instances,
                                                   include_failed_instances, include_killed_instances)
    if instance_id is None:
        return 1
    return ssh_instance_id(clusters, instance_id, command, container_name)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh',
                        help='ssh to a Waiter instance',
                        description='ssh to an instance given the token, service id, or instance id. Working directory '
                                    'will be the log directory.')
    parser.add_argument('token-or-service-id-or-instance-id')
    parser.add_argument('--container-name', '-c',
                        help='specify the container name you want to ssh into. Defaults to "waiter-app". Has no '
                             'effect if instance is not k8s pod.')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='ssh_destination', action='store_const', const=Destination.TOKEN,
                          default=Destination.TOKEN, help='Default; ssh with token')
    id_group.add_argument('--service-id', '-s', dest='ssh_destination', action='store_const',
                          const=Destination.SERVICE_ID, help='ssh using a service id')
    id_group.add_argument('--instance-id', '-i', dest='ssh_destination', action='store_const',
                          const=Destination.INSTANCE_ID, help='ssh directly to instance id')
    parser.add_argument('--quick', '-q', dest='quick', action='store_true',
                        help='skips services prompt by selecting the service with latest request, and instances prompt '
                             'by selecting a random one.')
    active_group = parser.add_mutually_exclusive_group(required=False)
    failed_group = parser.add_mutually_exclusive_group(required=False)
    killed_group = parser.add_mutually_exclusive_group(required=False)
    active_group.add_argument('--active', '-a', dest='include_active_instances', action='store_true', default=True,
                              help='included by default; includes active instances when prompting')
    failed_group.add_argument('--failed', '-f', dest='include_failed_instances', action='store_true',
                              help='includes failed instances when prompting')
    killed_group.add_argument('--killed', '-k', dest='include_killed_instances', action='store_true',
                              help='includes killed instances when prompting')
    active_group.add_argument('--no-active', dest='include_active_instances', action='store_false',
                              help="don't show active instances in prompt")
    failed_group.add_argument('--no-failed', dest='include_failed_instances', action='store_false',
                              help="don't show failed instances in prompt")
    killed_group.add_argument('--no-killed', dest='include_killed_instances', action='store_false',
                              help="don't show killed instances in prompt")
    parser.add_argument('command', nargs=argparse.REMAINDER, help='command to be run on instance')
    return ssh
