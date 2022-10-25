from waiter.util import guard_no_cluster, check_positive
from waiter.action import process_sigkill_request

def sigkill(clusters, args, _, __):
    """Send sigkill request to the specific instance"""
    guard_no_cluster(clusters)
    instance_id = args.get('instance-id')
    timeout_secs = args['timeout']
    params = {}
    success = process_sigkill_request(clusters, instance_id, timeout_secs)
    return 0 if success else 1

def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('sigkill', help='sigkill request to specific instance')
    parser.add_argument('instance-id')

    parser.add_argument('--timeout', '-t', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=30)

    return sigkill
