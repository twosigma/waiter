import functools
import importlib
import inspect
import json
import logging
import os
import random
import string
import unittest
import uuid
from datetime import datetime

import requests
import yaml
from retrying import retry

session = importlib.import_module(os.getenv('WAITER_TEST_SESSION_MODULE', 'requests')).Session()

# default time limit for each individual integration test
# if a test takes more than 10 minutes, it's probably broken
DEFAULT_TEST_TIMEOUT_SECS = int(os.getenv('WAITER_TEST_DEFAULT_TEST_TIMEOUT_SECS', 600))

# default time limit used by wait_until utility function
# 2 minutes should be more than sufficient on most cases
DEFAULT_TIMEOUT_MS = int(os.getenv('WAITER_TEST_DEFAULT_TIMEOUT_MS', 120000))

# default wait interval (i.e. time between attempts) used by wait_until utility function
DEFAULT_WAIT_INTERVAL_MS = int(os.getenv('WAITER_TEST_DEFAULT_WAIT_INTERVAL_MS', 1000))

# prefix to be used for tokens created during the integration tests
TOKEN_PREFIX = os.getenv('WAITER_TEST_TOKEN_PREFIX', 'cli')


def get_random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


class WaiterTest(unittest.TestCase):
    def token_name(self):
        """
        Returns the name of the currently running test function
        with a timestamp and 8-character random string
        """
        test_id = self.id()
        test_function = test_id.split('.')[-1]
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        random_string = get_random_string(8)
        return f'{TOKEN_PREFIX}_{test_function}_{timestamp}_{random_string}'


@functools.lru_cache()
def retrieve_waiter_url(varname='WAITER_URL', value='http://localhost:9091'):
    cook_url = os.getenv(varname, value)
    logging.info('Using waiter url %s' % cook_url)
    return cook_url


def retrieve_waiter_settings(waiter_url):
    return session.get(f'{waiter_url}/settings').json()


def retrieve_waiter_cluster_name(waiter_url):
    return retrieve_waiter_settings(waiter_url)['cluster-config']['name']


def is_connection_error(exception):
    return isinstance(exception, requests.exceptions.ConnectionError)


@retry(retry_on_exception=is_connection_error, stop_max_delay=240000, wait_fixed=1000)
def _wait_for_waiter(waiter_url):
    logging.debug('Waiting for connection to waiter...')
    # if connection is refused, an exception will be thrown
    session.get(waiter_url)


def init_waiter_session(*waiter_urls):
    for waiter_url in waiter_urls:
        _wait_for_waiter(waiter_url)


def cid():
    return f'waiter-tests-{uuid.uuid4()}'


def delete_token(waiter_url, token_name, assert_response=True, expected_status_code=200, kill_services=False):
    if kill_services:
        kill_services_using_token(waiter_url, token_name)

    headers = {
        'X-Waiter-Token': token_name,
        'x-cid': cid()
    }
    response = session.delete(f'{waiter_url}/token', headers=headers)
    if assert_response:
        logging.debug(f'Response status code: {response.status_code}')
        assert expected_status_code == response.status_code, response.text
    return response


def __load_token(waiter_url, token_name, assert_response, expected_status_code, params):
    if params is None:
        params = {'include': 'metadata'}

    headers = {
        'Content-Type': 'application/json',
        'X-Waiter-Token': token_name,
        'x-cid': cid()
    }
    response = session.get(f'{waiter_url}/token', headers=headers, params=params)
    if assert_response:
        assert \
            expected_status_code == response.status_code, \
            f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
    return response


def load_token(waiter_url, token_name, assert_response=True, expected_status_code=200, params=None):
    response = __load_token(waiter_url, token_name, assert_response, expected_status_code, params)
    return response.json()


def load_token_with_headers(waiter_url, token_name, assert_response=True, expected_status_code=200, params=None):
    response = __load_token(waiter_url, token_name, assert_response, expected_status_code, params)
    return response.json(), response.headers


def log_curl(waiter_url, endpoint, headers, params, payload):
    curl_header_flags = ' '.join(f"-H '{k}: {v}'" for k, v in headers.items())
    curl_url = f'{waiter_url}{endpoint}?{"&".join((k + "=" + v) for k, v in params.items())}'
    logging.debug(f'curl -XPOST {curl_header_flags} -d \'{json.dumps(payload)}\' \'{curl_url}\'')


def post_token(waiter_url, token_name, token_definition, assert_response=True,
               expected_status_code=200, update_mode_admin=False, etag=None):
    headers = {
        'Content-Type': 'application/json',
        'x-cid': cid()
    }
    params = {'token': token_name}
    if update_mode_admin:
        params['update-mode'] = 'admin'
        headers['If-Match'] = etag

    log_curl(waiter_url, '/token', headers, params, token_definition)
    response = session.post(f'{waiter_url}/token', headers=headers, json=token_definition, params=params)
    logging.debug(f'Response headers: {response.headers}')
    response_json = response.json()
    if assert_response:
        assert \
            expected_status_code == response.status_code, \
            f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
        fetched_token = load_token(waiter_url, token_name, params={})
        del fetched_token['owner']
        if 'cluster' in token_definition:
            del token_definition['cluster']
        assert \
            token_definition == fetched_token, \
            f'Expected {json.dumps(token_definition, indent=2)}, got {json.dumps(fetched_token, indent=2)}'
    return response_json


def minimal_service_cmd():
    this_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    kitchen = os.path.join(this_dir, os.pardir, os.pardir, os.pardir, os.pardir,
                           'containers', 'test-apps', 'kitchen', 'bin', 'kitchen')
    kitchen = os.path.abspath(kitchen)
    return f'{kitchen} -p $PORT0'


def minimal_service_description(**kwargs):
    service = {
        'cmd': default_cmd(),
        'cpus': float(os.getenv('WAITER_TEST_DEFAULT_CPUS', 1.0)),
        'mem': int(os.getenv('WAITER_TEST_DEFAULT_MEM_MB', 256)),
        'metric-group': 'waiter_test',
        'name': f'service_{get_random_string(10)}',
        'version': str(uuid.uuid4()),
        'cmd-type': 'shell'
    }
    service.update(kwargs)
    return service


def default_cmd():
    return os.getenv('WAITER_CLI_TEST_DEFAULT_CMD', minimal_service_cmd())


def wait_until(query, predicate, max_wait_ms=DEFAULT_TIMEOUT_MS, wait_interval_ms=DEFAULT_WAIT_INTERVAL_MS):
    """
    Block until the predicate is true for the result of the provided query.
    `query` is a thunk (nullary callable) that may be called multiple times.
    `predicate` is a unary callable that takes the result value of `query`
    and returns True if the condition is met, or False otherwise.
    """

    @retry(stop_max_delay=max_wait_ms, wait_fixed=wait_interval_ms)
    def wait_until_inner():
        response = query()
        if not predicate(response):
            error_msg = "wait_until condition not yet met, retrying..."
            logging.debug(error_msg)
            raise RuntimeError(error_msg)
        else:
            logging.info("wait_until condition satisfied")
            return response

    try:
        return wait_until_inner()
    except:
        final_response = query()
        try:
            details = final_response.content
        except AttributeError:
            details = str(final_response)
        logging.info(f"Timeout exceeded waiting for condition. Details: {details}")
        raise


def load_file(file_format, path):
    """Decode a JSON formatted file."""
    content = None

    if os.path.isfile(path):
        with open(path) as json_file:
            try:
                logging.debug(f'attempting to load {file_format} configuration from {path}')
                if file_format == 'json':
                    content = json.load(json_file)
                elif file_format == 'yaml':
                    content = yaml.safe_load(json_file)
                else:
                    raise Exception(f'Unsupported file format: {file_format}')
            except Exception as e:
                logging.error(e, f'error loading {file_format} configuration from {path}')
    else:
        logging.info(f'{path} is not a file')

    return content


def wait_until_routers(waiter_url, predicate):
    auth_cookie = {'x-waiter-auth': session.cookies['x-waiter-auth']}
    max_wait_ms = retrieve_waiter_settings(waiter_url)['scheduler-syncer-interval-secs'] * 2 * 1000
    routers = session.get(f'{waiter_url}/state/maintainer').json()['state']['routers']
    for _, router_url in routers.items():
        logging.debug(f'Waiting for at most {max_wait_ms}ms on {router_url}')
        wait_until(lambda: requests.get(f'{router_url.rstrip("/")}/apps', cookies=auth_cookie).json(),
                   predicate,
                   max_wait_ms=max_wait_ms)


def ping_token(waiter_url, token_name, expected_status_code=200):
    headers = {
        'Content-Type': 'application/json',
        'X-Waiter-Debug': 'true',
        'X-Waiter-Fallback-Period-Secs': '0',
        'X-Waiter-Token': token_name,
        'x-cid': cid()
    }
    response = session.get(f'{waiter_url}', headers=headers)
    assert \
        expected_status_code == response.status_code, \
        f'Expected {expected_status_code}, got {response.status_code} with body {response.text} '
    service_id = response.headers['x-waiter-service-id']
    wait_until_routers(waiter_url, lambda services: any(s['service-id'] == service_id for s in services))
    return service_id


def wait_until_routers_recognize_service_killed(waiter_url, service_id):
    wait_until_routers(waiter_url, lambda services: not any(s['service-id'] == service_id for s in services))


def kill_service(waiter_url, service_id):
    headers = {'x-cid': cid()}
    response = session.delete(f'{waiter_url}/apps/{service_id}', headers=headers)
    assert 200 == response.status_code, f'Expected 200, got {response.status_code} with body {response.text}'
    wait_until_routers_recognize_service_killed(waiter_url, service_id)


def services_for_token(waiter_url, token_name, assert_response=True, expected_status_code=200, log_services=False):
    headers = {
        'Content-Type': 'application/json',
        'x-cid': cid()
    }
    response = session.get(f'{waiter_url}/apps', headers=headers, params={'token': token_name})
    if assert_response:
        assert \
            expected_status_code == response.status_code, \
            f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
    services = response.json()
    logging.info(f'{len(services)} service(s) using token {token_name}')
    if log_services:
        logging.info(f'Services: {json.dumps(services, indent=2)}')
    return services


def instances_for_service(waiter_url, service_id, expected_status_code=200):
    headers = {
        'Content-Type': 'application/json',
        'x-cid': cid()
    }
    response = session.get(f'{waiter_url}/apps/{service_id}', headers=headers)
    service = response.json()
    assert expected_status_code == response.status_code, \
        f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
    return service['instances']


def multi_cluster_tests_enabled():
    """
    Returns true if the WAITER_TEST_MULTI_CLUSTER environment variable is set to "true",
    indicating that multiple Waiter instances are running.
    """
    return os.getenv('WAITER_TEST_MULTI_CLUSTER', None) == 'true'


def kill_services_using_token(waiter_url, token_name):
    services = services_for_token(waiter_url, token_name)
    for service in services:
        try:
            service_id = service['service-id']
            kill_service(waiter_url, service_id)
            wait_until(lambda: services_for_token(waiter_url, token_name),
                       lambda svcs: service_id not in [s['service-id'] for s in svcs])
        except:
            logging.exception(f'Encountered exception trying to kill service: {service}')


def wait_until_services_for_token(waiter_url, token_name, expected_num_services):
    return wait_until(lambda: services_for_token(waiter_url, token_name, log_services=True),
                      lambda svcs: len(svcs) == expected_num_services)


def wait_until_no_services_for_token(waiter_url, token_name):
    wait_until_services_for_token(waiter_url, token_name, 0)
