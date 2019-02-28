import logging
import uuid

import pytest

from tests.waiter import util, cli


@pytest.mark.cli
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)
class WaiterCliTest(util.WaiterTest):

    @classmethod
    def setUpClass(cls):
        cls.waiter_url = util.retrieve_waiter_url()
        util.init_waiter_session(cls.waiter_url)

    def setUp(self):
        self.waiter_url = type(self).waiter_url
        self.logger = logging.getLogger(__name__)

    def test_basic_create(self):
        token_name = self.current_name()
        util.delete_token_if_exists(self.waiter_url, token_name)
        cp = cli.create_minimal(self.waiter_url, token_name)
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token = util.load_token(self.waiter_url, token_name)
            self.assertIsNotNone(token)
            self.assertEqual('shell', token['cmd-type'])
            resp = util.session.get(self.waiter_url, headers={'X-Waiter-Token': token_name})
            self.assertEqual(200, resp.status_code, resp.text)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_failed_create(self):
        service = util.minimal_service_description(cpus=0)
        cp = cli.create_from_service_description(self.waiter_url, self.current_name(), service)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertEqual(b'Service description using waiter headers/token improperly configured\ncpus must be a '
                         b'positive number..\n', cp.stderr)

    def test_create_no_cluster(self):
        config = {'clusters': []}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp = cli.create_minimal(token_name=self.current_name(), flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('must specify at least one cluster', cli.decode(cp.stderr))

    def test_unspecified_create_cluster(self):
        config = {
            'clusters': [
                {"name": "Foo", "url": self.waiter_url},
                {"name": "Bar", "url": self.waiter_url}
            ]
        }
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp = cli.create_minimal(token_name=self.current_name(), flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('must either specify a cluster via --cluster or set "default-for-create" to true',
                          cli.decode(cp.stderr))

    def test_over_specified_create_cluster(self):
        config = {
            'clusters': [
                {"name": "Foo", "url": self.waiter_url, "default-for-create": True},
                {"name": "Bar", "url": self.waiter_url, "default-for-create": True}
            ]
        }
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp = cli.create_minimal(token_name=self.current_name(), flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('have "default-for-create" set to true for more than one cluster', cli.decode(cp.stderr))

    def test_single_specified_create_cluster(self):
        config = {
            'clusters': [
                {"name": "Foo", "url": str(uuid.uuid4())},
                {"name": "Bar", "url": self.waiter_url, "default-for-create": True}
            ]
        }
        with cli.temp_config_file(config) as path:
            token_name = self.current_name()
            util.delete_token_if_exists(self.waiter_url, token_name)
            flags = '--config %s' % path
            cp = cli.create_minimal(token_name=token_name, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            try:
                token = util.load_token(self.waiter_url, token_name)
                self.assertIsNotNone(token)
            finally:
                util.delete_token(self.waiter_url, token_name)

    def test_create_single_cluster(self):
        config = {'clusters': [{"name": "Bar", "url": self.waiter_url}]}
        with cli.temp_config_file(config) as path:
            token_name = self.current_name()
            util.delete_token_if_exists(self.waiter_url, token_name)
            flags = '--config %s' % path
            cp = cli.create_minimal(token_name=token_name, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            try:
                token = util.load_token(self.waiter_url, token_name)
                self.assertIsNotNone(token)
            finally:
                util.delete_token(self.waiter_url, token_name)

    def test_implicit_create_args(self):
        cp = cli.create(create_flags='--help')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('--cpus', cli.stdout(cp))
        self.assertNotIn('--https-redirect', cli.stdout(cp))
        self.assertNotIn('--fallback-period-secs', cli.stdout(cp))
        token_name = self.current_name()
        util.delete_token_if_exists(self.waiter_url, token_name)
        cp = cli.create(self.waiter_url, token_name, create_flags='--https-redirect true '
                                                                  '--cpus 0.1 '
                                                                  '--fallback-period-secs 10')
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token = util.load_token(self.waiter_url, token_name)
            self.assertTrue(token['https-redirect'])
            self.assertEqual(10, token['fallback-period-secs'])
            cp = cli.create(self.waiter_url, token_name, create_flags='--https-redirect false '
                                                                      '--cpus 0.1 '
                                                                      '--fallback-period-secs 20')
            self.assertEqual(0, cp.returncode, cp.stderr)
            token = util.load_token(self.waiter_url, token_name)
            self.assertFalse(token['https-redirect'])
            self.assertEqual(20, token['fallback-period-secs'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_basic_show(self):
        token_name = self.current_name()
        util.delete_token_if_exists(self.waiter_url, token_name)
        cp = cli.show(self.waiter_url, token_name)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Unable to retrieve token information', cli.decode(cp.stderr))
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            token = util.load_token(self.waiter_url, token_name)
            self.assertIsNotNone(token)
            cp = cli.show(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_implicit_show_fields(self):
        token_name = self.current_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'https-redirect': True, 'fallback-period-secs': 10})
        try:
            cp = cli.show(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Https redirect', cli.stdout(cp))
            self.assertIn('Fallback period (seconds)', cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name)
