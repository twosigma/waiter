
from waiter.util import guard_no_cluster, check_positive
from waiter.action import process_signal_request
from waiter.instance_select import Destination, get_instance_id_from_destination
from enum import Enum


class Signal(Enum):
    HARD_DELETE = 'hard-delete'
    SOFT_DELETE = 'soft-delete'
    

def signal(clusters, args, _, enforce_cluster):
    guard_no_cluster(clusters)

    token_or_service_id_or_instance_id = args.pop('token-or-service-id-or-instance-id')

    signal_destination = args.pop('signal_destination')

    instance_id = get_instance_id_from_destination(clusters, enforce_cluster, token_or_service_id_or_instance_id, signal_destination)
    if instance_id is None:
        return 0

    signal_type = args.pop('signal-type')
    timeout_secs = args['timeout']
    success = False

    if signal_type == Signal.HARD_DELETE.value:
        success = process_signal_request(clusters, "sigkill", instance_id, timeout_secs)
    elif signal_type == Signal.SOFT_DELETE.value:
        success = process_signal_request(clusters, "sigterm", instance_id, timeout_secs)
    else:
        success = False
    return 0 if success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('signal', help='sends signal to instance')
    parser.add_argument('signal-type', help='type of signal to send to instance')

    parser.add_argument('token-or-service-id-or-instance-id')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='signal_destination', action='store_const', const=Destination.TOKEN,
                          default=Destination.TOKEN, help='Default; signal with token')
    id_group.add_argument('--service-id', '-s', dest='signal_destination', action='store_const',
                          const=Destination.SERVICE_ID, help='signal using a service id')
    id_group.add_argument('--instance-id', '-i', dest='signal_destination', action='store_const',
                          const=Destination.INSTANCE_ID, help='signal directly to instance id')


    parser.add_argument('--timeout', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=0)    

    return signal
