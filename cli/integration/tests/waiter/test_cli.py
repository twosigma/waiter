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

        # Create token
        cp = cli.create_minimal(token_name, self.waiter_url)
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            # Make sure token now exists
            token = util.load_token(self.waiter_url, token_name)
            self.assertIsNotNone(token)
            self.assertEqual('shell', token['cmd-type'])

            # Make sure we can access the service
            resp = util.session.get(self.waiter_url, headers={'X-Waiter-Token': token_name})
            self.assertEqual(200, resp.status_code, resp.text)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_failed_create(self):
        service = util.minimal_service_description(cpus=0)
        cp = cli.create_from_service_description(self.current_name(), self.waiter_url, service)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertEqual(b'Service description using waiter headers/token improperly configured\ncpus must be a '
                         b'positive number..\n', cp.stderr)

    def test_no_cluster(self):
        config = {'clusters': []}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp = cli.create_minimal(self.current_name(), flags=flags)
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
            cp = cli.create_minimal(self.current_name(), flags=flags)
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
            cp = cli.create_minimal(self.current_name(), flags=flags)
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
            cp = cli.create_minimal(token_name, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            token = util.load_token(self.waiter_url, token_name)
            self.assertIsNotNone(token)

    def test_single_cluster(self):
        config = {'clusters': [{"name": "Bar", "url": self.waiter_url}]}
        with cli.temp_config_file(config) as path:
            token_name = self.current_name()
            util.delete_token_if_exists(self.waiter_url, token_name)
            flags = '--config %s' % path
            cp = cli.create_minimal(token_name, flags=flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            token = util.load_token(self.waiter_url, token_name)
            self.assertIsNotNone(token)
