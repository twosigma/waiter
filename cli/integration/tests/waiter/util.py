import functools
import importlib
import json
import logging
import os
import random
import string
import uuid
from datetime import datetime

import unittest

import requests
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


class WaiterTest(unittest.TestCase):
    def token_name(self):
        """
        Returns the name of the currently running test function
        with a timestamp and 8-character random string
        """
        test_id = self.id()
        test_function = test_id.split('.')[-1]
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        return f'{test_function}_{timestamp}_{random_string}'


@functools.lru_cache()
def retrieve_waiter_url(varname='WAITER_URL', value='http://localhost:9091'):
    cook_url = os.getenv(varname, value)
    logging.info('Using waiter url %s' % cook_url)
    return cook_url


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


def delete_token(waiter_url, token_name, assert_response=True, expected_status_code=200):
    response = session.delete(f'{waiter_url}/token', headers={'X-Waiter-Token': token_name})
    if assert_response:
        logging.debug(f'Response status code: {response.status_code}')
        assert expected_status_code == response.status_code, response.text
    return response


def load_token(waiter_url, token_name, assert_response=True, expected_status_code=200):
    headers = {
        'X-Waiter-Token': token_name,
        'Content-Type': 'application/json'
    }
    params = {'include': 'metadata'}
    response = session.get(f'{waiter_url}/token', headers=headers, params=params)
    if assert_response:
        assert \
            expected_status_code == response.status_code, \
            f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
    return response.json()


def post_token(waiter_url, token_name, token_definition, assert_response=True, expected_status_code=200):
    headers = {'Content-Type': 'application/json'}
    response = session.post(f'{waiter_url}/token', headers=headers, json=token_definition, params={'token': token_name})
    if assert_response:
        assert \
            expected_status_code == response.status_code, \
            f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
    return response.json()


def minimal_service_cmd(response_text=None):
    if response_text is None:
        response_text = 'OK'
    return f'RESPONSE="HTTP/1.1 200 OK\\r\\n\\r\\n{response_text}"; ' \
           'while { printf "$RESPONSE"; } | nc -l "$PORT0"; do ' \
           '  echo "====="; ' \
           'done'


def minimal_service_description(**kwargs):
    service = {
        'cmd': os.getenv('WAITER_CLI_TEST_DEFAULT_CMD', minimal_service_cmd()),
        'cpus': float(os.getenv('WAITER_TEST_DEFAULT_CPUS', 1.0)),
        'mem': int(os.getenv('WAITER_TEST_DEFAULT_MEM_MB', 256)),
        'version': str(uuid.uuid4()),
        'cmd-type': 'shell'
    }
    service.update(kwargs)
    return service


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


def load_json_file(path):
    """Decode a JSON formatted file."""
    content = None

    if os.path.isfile(path):
        with open(path) as json_file:
            try:
                logging.debug(f'attempting to load json configuration from {path}')
                content = json.load(json_file)
            except Exception as e:
                logging.error(e, f'error loading json configuration from {path}')
    else:
        logging.info(f'{path} is not a file')

    return content


def ping_token(waiter_url, token_name, assert_response=False, expected_status_code=200):
    headers = {
        'X-Waiter-Token': token_name,
        'X-Waiter-Debug': 'true',
        'X-Waiter-Fallback-Period-Secs': '0',
        'Content-Type': 'application/json'
    }
    response = session.get(f'{waiter_url}', headers=headers)
    if assert_response:
        assert \
            expected_status_code == response.status_code, \
            f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
    return response


def kill_service(waiter_url, service_id, assert_response=True, expected_status_code=200):
    response = session.delete(f'{waiter_url}/apps/{service_id}')
    if assert_response:
        logging.debug(f'Response status code: {response.status_code}')
        assert expected_status_code == response.status_code, response.text
    return response


def services_for_token(waiter_url, token_name, assert_response=True, expected_status_code=200):
    headers = {'Content-Type': 'application/json'}
    response = session.get(f'{waiter_url}/apps', headers=headers, params={'token': token_name})
    if assert_response:
        assert \
            expected_status_code == response.status_code, \
            f'Expected {expected_status_code}, got {response.status_code} with body {response.text}'
    return response.json()

  
def multi_cluster_tests_enabled():
    """
    Returns true if the WAITER_TEST_MULTI_CLUSTER environment variable is set to "true",
    indicating that multiple Waiter instances are running.
    """
    return os.getenv('WAITER_TEST_MULTI_CLUSTER', None) == 'true'
