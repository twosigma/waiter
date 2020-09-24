import importlib
import json
import logging
import uuid
from urllib.parse import urljoin

import requests

import waiter
from waiter.util import print_error


session = None
timeouts = None
adapter_factory = None


def set_retries(retries):
    """Sets the number of retries to use"""
    adapter = adapter_factory(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)


def configure(config, plugins):
    """Configures HTTP timeouts and retries to be used"""
    global session
    global timeouts
    global adapter_factory
    adapter_factory = plugins.get('http-adapter-factory', requests.adapters.HTTPAdapter)
    session_factory = plugins.get('http-session-factory', requests.Session)
    logging.getLogger('urllib3').setLevel(logging.DEBUG) # logging.disable in cli.py may override
    http_config = config.get('http')
    connect_timeout = http_config.get('connect-timeout')
    read_timeout = http_config.get('read-timeout')
    timeouts = (connect_timeout, read_timeout)
    logging.debug('using http timeouts: %s', timeouts)
    retries = http_config.get('retries')
    session = session_factory()
    set_retries(retries)
    session.headers['User-Agent'] = f"waiter/{waiter.version.VERSION} ({session.headers['User-Agent']})"
    auth_config = http_config.get('auth', None)
    if auth_config:
        auth_type = auth_config.get('type')
        if auth_type == 'basic':
            basic_auth_config = auth_config.get('basic')
            user = basic_auth_config.get('user')
            session.auth = (user, basic_auth_config.get('pass'))
            logging.debug(f'using http basic auth with user {user}')
        else:
            raise Exception(f'Encountered unsupported authentication type "{auth_type}".')


def __post(url, json_body, params=None, **kwargs):
    """Sends a POST with the json payload to the given url"""
    logging.debug(f'POST {url} with body {json_body} and headers {kwargs.get("headers", {})}')
    return session.post(url, json=json_body, timeout=timeouts, params=params, **kwargs)


def __get(url, params=None, read_timeout=None, **kwargs):
    """Sends a GET with params to the given url"""
    logging.debug(f'GET {url} with params {params} and headers {kwargs.get("headers", {})}')
    get_timeouts = timeouts
    if read_timeout is not None:
        get_timeouts = (timeouts[0], read_timeout)
    return session.get(url, params=params, timeout=get_timeouts, **kwargs)


def __delete(url, params=None, headers=None, read_timeout=None):
    """Sends a DELETE with params to the given url"""
    logging.debug(f'DELETE {url} with params {params} and headers {headers}')
    delete_timeouts = timeouts
    if read_timeout is not None:
        delete_timeouts = (timeouts[0], read_timeout)
    return session.delete(url, params=params, timeout=delete_timeouts, headers=headers)


def __make_url(cluster, endpoint):
    """Given a cluster and an endpoint, returns the corresponding full URL"""
    return urljoin(cluster['url'], endpoint)


def default_http_headers():
    """Returns the default HTTP headers, including a random CID in x-cid"""
    return {
        'Accept': 'application/json',
        'x-cid': f'waiter-{waiter.version.VERSION}-{uuid.uuid4()}'
    }


def post(cluster, endpoint, json_body, params=None, headers=None):
    """POSTs data to cluster at /endpoint"""
    if headers is None:
        headers = {}
    url = __make_url(cluster, endpoint)
    default_headers = default_http_headers()
    resp = __post(url, json_body, params=params, headers={**default_headers, **headers})
    resp.headers.pop('Set-Cookie', None)
    logging.info(f'POST response: {resp.text} (headers: {resp.headers})')
    return resp


def get(cluster, endpoint, params=None, headers=None, read_timeout=None):
    """GETs data corresponding to the given params from cluster at /endpoint"""
    if headers is None:
        headers = {}
    url = __make_url(cluster, endpoint)
    default_headers = default_http_headers()
    resp = __get(url, params, headers={**default_headers, **headers}, read_timeout=read_timeout)
    resp.headers.pop('Set-Cookie', None)
    logging.info(f'GET response: {resp.text} (headers: {resp.headers})')
    return resp


def delete(cluster, endpoint, params=None, headers=None, read_timeout=None):
    """DELETEs data corresponding to the given params on cluster at /endpoint"""
    if headers is None:
        headers = {}
    url = __make_url(cluster, endpoint)
    default_headers = default_http_headers()
    resp = __delete(url, params, headers={**default_headers, **headers}, read_timeout=read_timeout)
    logging.info(f'DELETE response: {resp.text}')
    return resp


def make_data_request(cluster, make_request_fn):
    """
    Makes a request (using make_request_fn), parsing the
    assumed-to-be-JSON response and handling common errors
    """
    try:
        resp = make_request_fn()
        if resp.status_code == 200:
            return resp.json(), resp.headers
        elif resp.status_code == 401:
            print_error(f'Authentication failed on {cluster["name"]} ({cluster["url"]}).')
            return [], {}
        elif resp.status_code == 500:
            print_error(f'Encountered server error while querying {cluster["name"]}.')
            # fall through to logging call below

        logging.warning(f'Unexpected response code {resp.status_code} for data request. Response body: {resp.text}')
    except requests.exceptions.ConnectionError as ce:
        logging.exception(ce)
        print_error(f'Encountered connection error with {cluster["name"]} ({cluster["url"]}).')
    except requests.exceptions.ReadTimeout as rt:
        logging.exception(rt)
        print_error(f'Encountered read timeout with {cluster["name"]} ({cluster["url"]}).')
    except IOError as ioe:
        logging.exception(ioe)
    except json.decoder.JSONDecodeError as jde:
        logging.exception(jde)
    return None, {}
