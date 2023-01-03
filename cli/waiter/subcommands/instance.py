from functools import partial

from waiter.action import process_signal_request
from waiter.instance_select import Destination, get_instance_id_from_destination
from waiter.util import check_positive, guard_no_cluster, logging


def kill_instance(clusters, args, _, enforce_cluster):
    guard_no_cluster(clusters)

    token_or_service_id_or_instance_id = args.pop('token-or-service-id-or-instance-id')

    signal_destination = args.pop('signal_destination')

    instance_id = get_instance_id_from_destination(
        clusters, enforce_cluster, token_or_service_id_or_instance_id, signal_destination)
    if instance_id is None:
        return 1

    timeout_secs = args.get('timeout', None)
    force_flag = args.get('force', False)

    query_params = {'force': force_flag}
    if timeout_secs is not None:
        query_params['timeout'] = timeout_secs
    success = process_signal_request(clusters, instance_id, "kill", query_params)
    return 0 if success else 1


def register_kill(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='sends kill to selected instance')

    parser.add_argument('token-or-service-id-or-instance-id')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='signal_destination', action='store_const', const=Destination.TOKEN,
                          default=Destination.TOKEN, help='Default; signal with token')
    id_group.add_argument('--service-id', '-s', dest='signal_destination', action='store_const',
                          const=Destination.SERVICE_ID, help='signal using a service id')
    id_group.add_argument('--instance-id', '-i', dest='signal_destination', action='store_const',
                          const=Destination.INSTANCE_ID, help='signal directly to instance id')

    parser.add_argument('--force', '-f', help='kill all services, never prompt', dest='force', action='store_true')
    parser.add_argument('--timeout', help='timeout (in seconds) for kill to complete',
                        required=False, type=check_positive)
    parser.set_defaults(sub_func=kill_instance)

    return kill_instance


def signal_instance(parser, clusters, args, _, enforce_cluster):
    """Calls the sub-action for instance command.
    If no sub action is provided then displays the help message."""
    logging.debug('args: %s' % args)
    sub_func = args.get('sub_func', None)
    if sub_func is None:
        parser.print_help()
        return 0
    else:
        return sub_func(clusters, args, _, enforce_cluster)


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('instance',
                        help='manage instance(s) for a token or service',
                        description='Manage instance(s) for a token or service.')
    subparsers = parser.add_subparsers()
    register_kill(subparsers.add_parser)
    return partial(signal_instance, parser)
