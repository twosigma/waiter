
from retrying import retry, RetryError
from waiter import terminal
from waiter.action import check_ssl, process_ping_request
from waiter.util import check_positive, guard_no_cluster, print_error

_default_timeout = 300

def ready(clusters, args, _, __):
    """Ensure the Waiter service is ready for traffic."""
    guard_no_cluster(clusters)
    port = 443
    token_name = args.get('token')
    token_host = f'{token_name}:{port}'
    ping_timeout_secs = args.get('ping_timeout', _default_timeout)
    ssl_timeout_secs = args.get('connect_timeout', _default_timeout)
    wait_secs = 10 if ssl_timeout_secs > 10 else 1
    if not process_ping_request(clusters, token_name, False, ping_timeout_secs, True):
        return 1
    retry_options = {
        'retry_on_result': lambda r: not r,
        'stop_max_delay': ssl_timeout_secs * 1000,
        'wait_fixed': wait_secs * 1000,
    }
    print(f'Awaiting successful connection to {terminal.bold(token_host)}')
    check_ssl_with_retries = retry(**retry_options)(check_ssl)
    try:
        check_ssl_with_retries(token_name, port, ssl_timeout_secs)
    # Any exception indicates an issue connecting to or handshaking the backend service
    except Exception as e:
        print_error(e)
        print_error(f'Failed to connect to {token_host} after {ssl_timeout_secs} seconds')
        return 1
    return 0


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ready', help='ensure the target token is ready for traffic')
    parser.add_argument('token')
    parser.add_argument('--ping-timeout', help=f'timeout (in seconds) for ping request via waiter api'
                        f' (default is {_default_timeout} seconds)',
                        type=check_positive, default=_default_timeout)
    parser.add_argument('--connect-timeout', help=f'timeout (in seconds) for tls connection to service'
                        f' (default is {_default_timeout} seconds)',
                        type=check_positive, default=_default_timeout)
    return ready

