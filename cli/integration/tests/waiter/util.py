import functools
import importlib
import logging
import os

import unittest

import requests
from retrying import retry

session = importlib.import_module(os.getenv('WAITER_TEST_SESSION_MODULE', 'requests')).Session()

# default time limit for each individual integration test
# if a test takes more than 10 minutes, it's probably broken
DEFAULT_TEST_TIMEOUT_SECS = int(os.getenv('WAITER_TEST_DEFAULT_TEST_TIMEOUT_SECS', 600))


class WaiterTest(unittest.TestCase):
    def current_name(self):
        """Returns the name of the currently running test function"""
        test_id = self.id()
        return test_id.split('.')[-1]


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
    response = session.get(f'{waiter_url}/token', headers=headers)
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
        'cmd': os.getenv('WAITER_TEST_DEFAULT_CMD', minimal_service_cmd()),
        'cpus': float(os.getenv('WAITER_TEST_DEFAULT_CPUS', 1.0)),
        'mem': int(os.getenv('WAITER_TEST_DEFAULT_MEM_MB', 256)),
        'version': 'version-does-not-matter',
        'cmd-type': 'shell'
    }
    service.update(kwargs)
    return service
