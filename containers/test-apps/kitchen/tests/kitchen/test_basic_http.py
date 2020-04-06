import concurrent.futures
import itertools
import logging
import pytest
import requests
import tenacity
import threading
import time

from tests.kitchen import util

def lorem_ipsum(length):
    text = b'Lorem ipsum dolor sit amet, proin in nibh tellus penatibus, viverra nunc risus ligula proin ligula.'
    return bytes(itertools.islice(itertools.cycle(text), length)).decode('ascii')

@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)  # individual test timeout
class TestBasicHttp:

    def test_hello(self, kitchen_server):
        """Test default 'Hello World' response"""
        req = requests.get(kitchen_server.url())
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'text/plain'
        assert req.text == 'Hello World'

    def test_hello_https(self, kitchen_ssl_server):
        """Test default 'Hello World' response with HTTPS"""
        req = requests.get(kitchen_ssl_server.url(), verify='.')
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'text/plain'
        assert req.text == 'Hello World'

    def test_content_type(self, kitchen_server):
        """Test default 'Hello World' response"""
        req = requests.get(kitchen_server.url(), headers={'x-kitchen-content-type': 'text/html'})
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'text/html'

    def test_consume_chunked_post(self, kitchen_server):
        """Test that a large chunked POST payload is completely consumed before connection is closed"""
        total_chunks = 100
        chunk_size = 100000  # 100KB
        chunk = b'A' * chunk_size

        def chunked_payload():
            for i in range(total_chunks):
                yield chunk

        req = requests.post(kitchen_server.url('/request-info'), data=chunked_payload())
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'application/json'
        response_json = req.json()
        assert response_json['headers'].get('content-length') is None, response_json
        assert response_json['headers'].get('transfer-encoding') == 'chunked', response_json

    def test_consume_unchunked_post(self, kitchen_server):
        """Test that a large unchunked POST payload is completely consumed before connection is closed"""
        payload_size = 100000000  # 100MB
        payload = b'A' * payload_size
        req = requests.post(kitchen_server.url('/request-info'), data=payload)
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'application/json'
        response_json = req.json()
        assert response_json['headers'].get('content-length') == str(payload_size), response_json
        assert response_json['headers'].get('transfer-encoding') is None, response_json

    def test_multithreaded_server(self, kitchen_server):
        """Test that the server can handle overlapping requests"""
        started_signal = threading.Event()
        finish_signal = threading.Event()

        def delayed_payload():
            yield b'Delayed '
            started_signal.set()
            finish_signal.wait()
            yield b'Payload'

        def make_long_request():
            req = requests.post(
                    kitchen_server.url('/'),
                    headers={'x-kitchen-echo': 'true'},
                    data=delayed_payload())
            assert req.status_code == requests.codes.ok
            assert req.headers.get('Content-Type') == 'text/plain'
            assert req.text == 'Delayed Payload'

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            long_request_future = executor.submit(make_long_request)
            started_signal.wait()
            try:
                req = requests.get(kitchen_server.url('/kitchen-state'))
                assert req.status_code == requests.codes.ok
                assert req.headers.get('Content-Type') == 'application/json'
                assert req.json().get('pending-http-requests') == 1
            finally:
                finish_signal.set()
            long_request_future.result()

    def test_killed_request(self, kitchen_server):
        """Test that the server cleans up after a request that closes mid-stream"""
        started_signal = threading.Event()
        finish_signal = threading.Event()

        def delayed_payload():
            yield b'Delayed '
            started_signal.set()
            finish_signal.wait()
            raise Exception('close connection')

        def make_long_request():
            requests.post(
                    kitchen_server.url('/'),
                    headers={'x-kitchen-echo': 'true'},
                    data=delayed_payload())

        def await_n_pending_requests(n, max_wait_secs):
            @tenacity.retry(stop=tenacity.stop_after_delay(max_wait_secs), wait=tenacity.wait_fixed(1))
            def await_helper():
                req = requests.get(kitchen_server.url('/kitchen-state'))
                assert req.status_code == requests.codes.ok
                assert req.headers.get('Content-Type') == 'application/json'
                logging.info('n = {}'.format(req.json().get('pending-http-requests')))
                assert req.json().get('pending-http-requests') == n
            await_helper()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            long_request_future = executor.submit(make_long_request)
            started_signal.wait(timeout=10)
            try:
                await_n_pending_requests(1, max_wait_secs=0)
            finally:
                finish_signal.set()
            await_n_pending_requests(0, max_wait_secs=10)

    def test_chunked_encoding(self, kitchen_server):
        """Test for valid chunked encoding of response payloads"""
        n = 1024 * 1024
        req = requests.get(kitchen_server.url('/chunked'), headers={'x-kitchen-response-size': str(n)})
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'text/plain'
        assert req.headers.get('Transfer-Encoding') == 'chunked'
        assert len(req.content) == n
        assert req.text == lorem_ipsum(n)

    def test_gzip_encoding(self, kitchen_server):
        """Test for valid gzip encoding of response payloads"""
        n = 1024 * 1024
        req = requests.get(kitchen_server.url('/gzip'), headers={'x-kitchen-response-size': str(n)})
        assert req.status_code == requests.codes.ok
        assert req.headers.get('Content-Type') == 'text/plain'
        assert req.headers.get('Content-Encoding') == 'gzip'
        assert len(req.content) == n
        assert req.text == lorem_ipsum(n)
