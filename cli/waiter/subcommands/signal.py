from waiter.util import guard_no_cluster, check_positive
from waiter.action import process_sigkill_request

class SIGNAL(Enum):
    SIGKILL = 'sigkill'

def sigkill(clusters, signal_type, instance_id, timeout_secs, _, __):
    """Send sigkill request to the specific instance"""
    guard_no_cluster(clusters)
    params = {}
    success = process_sigkill_request(clusters, signal_type, instance_id, timeout_secs)
    return 0 if success else 1

def signal(clusters, args, _, __):
    guard_no_cluster(clusters)
#     token_or_service_id_or_instance_id = args.pop('token-or-service-id-or-instance-id')
#     command = args.pop('command')
    signal_type = args['signal-type']
    instance_id = args.pop('instance-id')
    timeout_secs = args['timeout']
#     include_active_instances = args.pop('include_active_instances')
#     include_failed_instances = args.pop('include_failed_instances')
#     include_killed_instances = args.pop('include_killed_instances')
#     container_name = args.pop('container_name', 'waiter-app')
#     skip_prompts = args.pop('quick')
    if signal_type == SIGNAL.SIGKILL:
        return sigkill(clusters, signal_type, instance_id, timeout_secs)

def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('signal', help='sends signal to instance')
    parser.add_argument('instance-id')
    parser.add_argument('--signal-type', help='specify what signal type to send',
                        default='sigkill')
    parser.add_argument('--timeout', '-t', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=30)

    return signal
