import logging
import unittest
import uuid

import pytest

from tests.waiter import cli, util


@pytest.mark.cli
@unittest.skipUnless(util.multi_cluster_tests_enabled(), 'Requires setting WAITER_TEST_MULTI_CLUSTER')
@pytest.mark.timeout(util.DEFAULT_TEST_TIMEOUT_SECS)
class MultiWaiterCliTest(util.WaiterTest):

    @classmethod
    def setUpClass(cls):
        cls.waiter_url_1 = util.retrieve_waiter_url()
        cls.waiter_url_2 = util.retrieve_waiter_url('WAITER_URL_2', 'http://localhost:9191')
        util.init_waiter_session(cls.waiter_url_1, cls.waiter_url_2)
        cli.write_base_config()

    def setUp(self):
        self.waiter_url_1 = type(self).waiter_url_1
        self.waiter_url_2 = type(self).waiter_url_2
        self.logger = logging.getLogger(__name__)

    def __two_cluster_config(self):
        return {'clusters': [{'name': 'waiter1', 'url': self.waiter_url_1},
                             {'name': 'waiter2', 'url': self.waiter_url_2}]}

    def test_federated_show(self):
        # Create in cluster #1
        token_name = self.token_name()
        version_1 = str(uuid.uuid4())
        util.post_token(self.waiter_url_1, token_name, {'version': version_1})
        try:
            # Single query for the token name, federated across clusters
            config = self.__two_cluster_config()
            with cli.temp_config_file(config) as path:
                cp, tokens = cli.show_token(token_name=token_name, flags='--config %s' % path)
                versions = [t['version'] for t in tokens]
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertEqual(1, len(tokens), tokens)
                self.assertIn(version_1, versions)

                # Create in cluster #2
                version_2 = str(uuid.uuid4())
                util.post_token(self.waiter_url_2, token_name, {'version': version_2})
                try:
                    # Again, single query for the token name, federated across clusters
                    cp, tokens = cli.show_token(token_name=token_name, flags='--config %s' % path)
                    versions = [t['version'] for t in tokens]
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertEqual(2, len(tokens), tokens)
                    self.assertIn(version_1, versions)
                    self.assertIn(version_2, versions)
                finally:
                    util.delete_token(self.waiter_url_2, token_name)
        finally:
            util.delete_token(self.waiter_url_1, token_name)

    def test_federated_delete(self):
        # Create in cluster #1
        token_name = self.token_name()
        version_1 = str(uuid.uuid4())
        util.post_token(self.waiter_url_1, token_name, {'version': version_1})
        try:
            # Create in cluster #2
            version_2 = str(uuid.uuid4())
            util.post_token(self.waiter_url_2, token_name, {'version': version_2})
            try:
                config = self.__two_cluster_config()
                with cli.temp_config_file(config) as path:
                    # Delete the token in both clusters
                    cp = cli.delete(token_name=token_name, flags='--config %s' % path, delete_flags='--force')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn('exists in 2 clusters', cli.stdout(cp))
                    self.assertIn('waiter1', cli.stdout(cp))
                    self.assertIn('waiter2', cli.stdout(cp))
                    self.assertEqual(2, cli.stdout(cp).count('Deleting token'))
                    self.assertEqual(2, cli.stdout(cp).count('Successfully deleted'))
                    util.load_token(self.waiter_url_1, token_name, expected_status_code=404)
                    util.load_token(self.waiter_url_2, token_name, expected_status_code=404)
            finally:
                util.delete_token(self.waiter_url_2, token_name, assert_response=False)
        finally:
            util.delete_token(self.waiter_url_1, token_name, assert_response=False)
