from waiter.util import guard_no_cluster, check_positive
from waiter.action import process_signal_request
from waiter.display import get_user_selection, tabulate_service_instances
from waiter.querying import query_service
from enum import Enum

class Signal(Enum):
    SIGKILL = 'sigkill'
    SIGTERM = 'sigterm'

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

def signal(clusters, args, _, __):
    guard_no_cluster(clusters)

    service_id = args.pop('service-id')

    instances = get_instances_from_service_id(clusters, service_id, True, False, False)
    column_names = ['Instance Id', 'Host', 'Status']
    tabular_output = tabulate_service_instances(instances, show_index=True, column_names=column_names)
    selected_instance = get_user_selection(instances, tabular_output)
    
    instance_id = selected_instance.get('id')
    signal_type = args.pop('signal_type')

    timeout_secs = args['timeout']

    success = False

    if signal_type == Signal.SIGKILL.value:
        success = process_signal_request(clusters, signal_type, instance_id, timeout_secs)
    elif signal_type == Signal.SIGTERM.value:
        success = process_signal_request(clusters, signal_type, instance_id, timeout_secs)

    return 0 if success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('signal', help='sends signal to instance')
    parser.add_argument('signal_type', help='type of signal to send to instance')
    # parser.add_argument('instance-id')
    parser.add_argument('service-id')
    parser.add_argument('--timeout', '-t', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=30)

    return signal
