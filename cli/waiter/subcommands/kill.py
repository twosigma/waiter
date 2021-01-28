from waiter.action import process_kill_request
from waiter.util import guard_no_cluster, check_positive


def kill(clusters, args, _, __):
    """Kills the service(s) using the given token name."""
    guard_no_cluster(clusters)
    token_name_or_service_id = args.get('token-or-service-id')
    is_service_id = args.get('is-service-id', False)
    force_flag = args.get('force', False)
    timeout_secs = args['timeout']
    success = process_kill_request(clusters, token_name_or_service_id, is_service_id, force_flag, timeout_secs)
    return 0 if success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('kill', help='kill services')
    parser.add_argument('token-or-service-id')
    parser.add_argument('--force', '-f', help='kill all services, never prompt', dest='force', action='store_true')
    parser.add_argument('--service-id', '-s', help='kill by service id instead of token',
                        dest='is-service-id', action='store_true')
    parser.add_argument('--timeout', '-t', help='timeout (in seconds) for kill to complete',
                        type=check_positive, default=30)
    return kill
