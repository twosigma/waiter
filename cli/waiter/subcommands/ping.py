
from waiter.action import process_ping_request
from waiter.util import check_positive, guard_no_cluster


def ping(clusters, args, _, __):
    """Pings the token with the given token name."""
    guard_no_cluster(clusters)
    token_name_or_service_id = args.get('token-or-service-id')
    is_service_id = args.get('is-service-id', False)
    timeout_secs = args.get('timeout', None)
    wait_for_request = args.get('wait', True)
    ping_success = process_ping_request(clusters, token_name_or_service_id, is_service_id,
                                        timeout_secs, wait_for_request)
    return 0 if ping_success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    default_timeout = 300
    parser = add_parser('ping', help='ping token by name')
    parser.add_argument('token-or-service-id')
    parser.add_argument('--timeout', '-t', help=f'read timeout (in seconds) for ping request (default is '
                                                f'{default_timeout} seconds)',
                        type=check_positive, default=default_timeout)
    parser.add_argument('--service-id', '-s', help='ping by service id instead of token',
                        dest='is-service-id', action='store_true')
    parser.add_argument('--no-wait', '-n', help='do not wait for ping request to return',
                        dest='wait', action='store_false')
    return ping
