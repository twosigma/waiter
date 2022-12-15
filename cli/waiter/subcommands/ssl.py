
from waiter.action import check_ssl
from waiter.util import check_positive


def ensureSSL(_, args, __, ___):
    """Checks the state of SSL for the provided token."""
    token_name = args.get('token')
    timeout_secs = args.get('timeout', None)
    ssl_success = check_ssl(token_name, timeout_secs)
    return 0 if ssl_success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    default_timeout = 300
    parser = add_parser('ensure-ssl', help='checks if the specified token has SSL set up')
    parser.add_argument('token')
    parser.add_argument('--timeout', '-t', help=f'read timeout (in seconds) for SSL verification request (default is '
                                                f'{default_timeout} seconds)',
                        type=check_positive, default=default_timeout)
    return ensureSSL
