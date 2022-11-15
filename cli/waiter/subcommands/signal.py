from waiter.util import guard_no_cluster, check_positive
from waiter.action import process_signal_request
from waiter.display import get_user_selection, tabulate_service_instances, tabulate_token_services
from waiter.querying import print_no_data, print_no_services, query_service, query_token, print_no_instances
from enum import Enum


class Signal(Enum):
    SIGKILL = 'sigkill'
    SIGTERM = 'sigterm'

class Destination(Enum):
    TOKEN = 'token'
    SERVICE_ID = 'service_id'
    INSTANCE_ID = 'instance_id'

def signal_service_id(clusters, service_id):
    instances = get_instances_from_service_id(clusters, service_id)
    if instances is False:
        print_no_data(clusters)
        return 1
    if len(instances) == 0:
        print_no_instances(service_id)
        return 1
    else:
        column_names = ['Instance Id', 'Host', 'Status']
        tabular_output = tabulate_service_instances(instances, show_index=True, column_names=column_names)
        selected_instance = get_user_selection(instances, tabular_output)
    return selected_instance

def signal_token(clusters, token):
    query_result = query_token(clusters, token, include_services=True)
    if query_result['count'] == 0:
        print_no_data(clusters)
        return None
    clusters_by_name = {c['name']: c for c in clusters}
    cluster_data = query_result['clusters']
    services = [{'cluster': cluster, 'etag': data['etag'], **service}
                for cluster, data in cluster_data.items()
                for service in data['services']]
    if len(services) == 0:
        print_no_services(clusters, token)
        return None
    column_names = ['Service Id', 'Cluster', 'Instances', 'In-flight req.', 'Status', 'Last request', 'Current?']
    tabular_output, sorted_services = tabulate_token_services(services, token, show_index=True, summary_table=False,
                                                                column_names=column_names)
    selected_service = get_user_selection(sorted_services, tabular_output)
    selected_service_id = selected_service['service-id']
    clusters = [clusters_by_name[selected_service['cluster']]]
    return signal_service_id(clusters, selected_service_id)


def map_instances_with_status(instances, status):
    return [{'_status': status, **inst} for inst in instances]


def get_instances_from_service_id(clusters, service_id):
    query_result = query_service(clusters, service_id)
    num_services = query_result['count']
    if num_services == 0:
        return False
    services = map(lambda service_data: service_data['service'], query_result['clusters'].values())
    instances = []
    for service in services:
        instances += map_instances_with_status(service['instances']['active-instances'], 'active')
    return instances

def signal(clusters, args, _, __):
    guard_no_cluster(clusters)

    token_or_service_id_or_instance_id = args.pop('token-or-service-id-or-instance-id')

    signal_destination = args.pop('signal_destination')

    instance_id = None
    if signal_destination == Destination.TOKEN:
        selected_instance = signal_token(clusters, token_or_service_id_or_instance_id)
        if selected_instance != None:
            instance_id = selected_instance.get('id', None)
        else:
            return 0
    elif signal_destination == Destination.SERVICE_ID:
        selected_instance = signal_service_id(clusters, token_or_service_id_or_instance_id)
        if selected_instance != None:
            instance_id = selected_instance.get('id', None)
        else:
            return 0
    elif signal_destination == Destination.INSTANCE_ID:
        instance_id = token_or_service_id_or_instance_id
    
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

    parser.add_argument('token-or-service-id-or-instance-id')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='signal_destination', action='store_const', const=Destination.TOKEN,
                          default=Destination.TOKEN, help='Default; signal with token')
    id_group.add_argument('--service-id', '-s', dest='signal_destination', action='store_const',
                          const=Destination.SERVICE_ID, help='signal using a service id')
    id_group.add_argument('--instance-id', '-i', dest='signal_destination', action='store_const',
                          const=Destination.INSTANCE_ID, help='signal directly to instance id')


    parser.add_argument('--timeout', '-ti', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=30)    

    return signal
