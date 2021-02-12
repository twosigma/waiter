import argparse
import os

from waiter.querying import print_no_services, query_services
from waiter.util import guard_no_cluster


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
        if num_services == 0:
            print_no_services(clusters, token_or_service_id_or_pod_name)
            return 1
        clusters_with_services =
        if len(query_result.keys) > 1:
            # get cluster selection
        else:
            cluster = query_result.keys()[0]
        services = query_result[cluster]
        if len(services) > 1:
            # get service selection
        else:
            service = services[0]
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
