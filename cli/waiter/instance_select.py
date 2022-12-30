from enum import Enum

from waiter.display import get_user_selection, tabulate_service_instances, tabulate_token_services
from waiter.querying import get_service_id_from_instance_id, get_target_cluster_from_token, print_no_data, \
    print_no_services, query_service, query_token, get_services_on_cluster, print_no_instances


class Destination(Enum):
    TOKEN = 'token'
    SERVICE_ID = 'service_id'
    INSTANCE_ID = 'instance_id'


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


def select_from_instance_id(clusters, instance_id):
    service_id = get_service_id_from_instance_id(instance_id)
    instances = get_instances_from_service_id(clusters, service_id, True, True, True)
    if instances is False:
        print_no_data(clusters)
        return None
    found_instance = next((instance
                           for instance in instances
                           if instance['id'] == instance_id),
                          False)
    if not found_instance:
        print_no_data(clusters)
        return None
    return instance_id


def select_from_service_id(clusters, service_id, skip_prompts, include_active_instances,
                           include_failed_instances, include_killed_instances):
    instances = get_instances_from_service_id(clusters, service_id, include_active_instances, include_failed_instances,
                                              include_killed_instances)
    if instances is False:
        print_no_data(clusters)
        return None
    if len(instances) == 0:
        print_no_instances(service_id)
        return None
    if skip_prompts:
        selected_instance = instances[0]
    else:
        column_names = ['Instance Id', 'Host', 'Status']
        tabular_output = tabulate_service_instances(instances, show_index=True, column_names=column_names)
        selected_instance = get_user_selection(instances, tabular_output)
    if selected_instance is not None:
        return selected_instance.get('id', None)
    return None


def select_from_token(clusters, enforce_cluster, token, skip_prompts, include_active_instances,
                      include_failed_instances, include_killed_instances):
    if skip_prompts:
        cluster = get_target_cluster_from_token(clusters, token, enforce_cluster)
        query_result = get_services_on_cluster(cluster, token)
        services = [s
                    for s in query_result.get('services', [])
                    if s['instance-counts']['healthy-instances'] + s['instance-counts']['unhealthy-instances'] > 0]
        if len(services) == 0:
            print_no_services(clusters, token)
            return None
        max_last_request = max(s.get('last-request-time', '') for s in services)
        selected_service_id = next(s['service-id'] for s in services if s['last-request-time'] == max_last_request)
    else:
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
    return select_from_service_id(clusters, selected_service_id, skip_prompts,
                                  include_active_instances, include_failed_instances, include_killed_instances)


def get_instance_id_from_destination(clusters, enforce_cluster, token_or_service_id_or_instance_id, destination,
                                     skip_prompts=False, include_active_instances=True, include_failed_instances=False,
                                     include_killed_instances=False):
    if destination.value == Destination.TOKEN.value:
        return select_from_token(clusters, enforce_cluster, token_or_service_id_or_instance_id,
                                 skip_prompts, include_active_instances, include_failed_instances,
                                 include_killed_instances)
    elif destination.value == Destination.SERVICE_ID.value:
        return select_from_service_id(clusters, token_or_service_id_or_instance_id, skip_prompts,
                                      include_active_instances, include_failed_instances, include_killed_instances)
    elif destination.value == Destination.INSTANCE_ID.value:
        return select_from_instance_id(clusters, token_or_service_id_or_instance_id)
    else:
        return None
