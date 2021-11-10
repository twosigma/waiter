from waiter.action import process_kill_request, process_ping_request
from waiter.util import guard_no_cluster, check_positive


def kill(clusters, args, _, __):
    """Kills the service(s) using the given token name."""
    guard_no_cluster(clusters)
    token_name_or_service_id = args.get('token-or-service-id')
    is_service_id = args.get('is-service-id', False)
    force_flag = args.get('force', False)
    timeout_secs = args['timeout']
    ping_token_name_or_service_id = args.get('ping_token', False)

    success = process_kill_request(clusters, token_name_or_service_id, is_service_id, force_flag, timeout_secs)
    if success and ping_token_name_or_service_id:
        ping_timeout_secs = args.get('ping_timeout')
        ping_wait = args.get('ping_wait', True)
        ping_success = process_ping_request(clusters, token_name_or_service_id, is_service_id,
                                            ping_timeout_secs, ping_wait)
        return 0 if ping_success else 1
    else:
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

    ping_group = parser.add_mutually_exclusive_group(required=False)
    ping_group.add_argument('--no-ping', action='store_false', dest='ping_token',
                            help='skips pinging the token/service; pinging the token is disabled by default')
    ping_group.add_argument('--ping', action='store_true', dest='ping_token',
                            help='pings the token/service after killing service(s).')
    parser.set_defaults(ping_token=False)
    parser.add_argument('--ping-timeout', default=300, dest='ping_timeout',
                        help='read timeout (in seconds) for ping request (default=300)', type=check_positive)
    ping_wait_group = parser.add_mutually_exclusive_group(required=False)
    ping_wait_group.add_argument('--no-ping-wait', action='store_false', dest='ping_wait',
                                 help='do not wait for ping request to return')
    ping_wait_group.add_argument('--ping-wait', action='store_true', dest='ping_wait',
                                 help='wait for ping request to return (default)')
    parser.set_defaults(ping_wait=True)

    return kill
