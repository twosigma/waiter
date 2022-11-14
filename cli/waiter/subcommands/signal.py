from waiter.util import guard_no_cluster, check_positive
from waiter.action import process_signal_request
from enum import Enum

class Signal(Enum):
    SIGKILL = 'sigkill'
    SIGTERM = 'sigterm'

def signal(clusters, args, _, __):
    guard_no_cluster(clusters)

    signal_type = args.get('signal_type')

    instance_id = args.pop('instance-id')

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
    parser.add_argument('instance-id')
#     parser.add_argument('--signal-type', help='specify what signal type to send',
#                         default='sigkill')
    parser.add_argument('signal_type')
    parser.add_argument('--timeout', '-t', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=30)

    return signal
