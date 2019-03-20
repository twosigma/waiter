import getpass
import logging
import os
import threading
import unittest
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
        cli.write_base_config()

    def setUp(self):
        self.waiter_url = type(self).waiter_url
        self.logger = logging.getLogger(__name__)

    def test_basic_create(self):
        token_name = self.token_name()
        version = str(uuid.uuid4())
        cmd = util.minimal_service_cmd()
        cp = cli.create_minimal(self.waiter_url, token_name, flags=None, cmd=cmd, cpus=0.1, mem=128, version=version)
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token_data = util.load_token(self.waiter_url, token_name)
            print(token_data)
            self.assertIsNotNone(token_data)
            self.assertEqual('shell', token_data['cmd-type'])
            self.assertEqual(cmd, token_data['cmd'])
            self.assertEqual(0.1, token_data['cpus'])
            self.assertEqual(128, token_data['mem'])
            self.assertEqual(getpass.getuser(), token_data['owner'])
            self.assertEqual(getpass.getuser(), token_data['last-update-user'])
            self.assertEqual({}, token_data['previous'])
            self.assertEqual(version, token_data['version'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_failed_create(self):
        service = util.minimal_service_description(cpus=0)
        cp = cli.create_from_service_description(self.waiter_url, self.token_name(), service)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Service description', cli.decode(cp.stderr))
        self.assertIn('improper', cli.decode(cp.stderr))
        self.assertIn('cpus must be a positive number', cli.decode(cp.stderr))

    def test_create_no_cluster(self):
        config = {'clusters': []}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp = cli.create_minimal(token_name=self.token_name(), flags=flags)
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
            cp = cli.create_minimal(token_name=self.token_name(), flags=flags)
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
            cp = cli.create_minimal(token_name=self.token_name(), flags=flags)
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
            token_name = self.token_name()
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
            token_name = self.token_name()
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
        self.assertNotIn('--idle-timeout-mins', cli.stdout(cp))
        self.assertNotIn('--max-instances', cli.stdout(cp))
        self.assertNotIn('--restart-backoff-factor', cli.stdout(cp))
        token_name = self.token_name()
        cp = cli.create(self.waiter_url, token_name, create_flags='--https-redirect true '
                                                                  '--cpus 0.1 '
                                                                  '--fallback-period-secs 10 '
                                                                  '--idle-timeout-mins 1 '
                                                                  '--max-instances 100 '
                                                                  '--restart-backoff-factor 1.1')
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token = util.load_token(self.waiter_url, token_name)
            self.assertTrue(token['https-redirect'])
            self.assertEqual(10, token['fallback-period-secs'])
            self.assertEqual(1, token['idle-timeout-mins'])
            self.assertEqual(100, token['max-instances'])
            self.assertEqual(1.1, token['restart-backoff-factor'])
            cp = cli.create(self.waiter_url, token_name, create_flags='--https-redirect false '
                                                                      '--cpus 0.1 '
                                                                      '--fallback-period-secs 20 '
                                                                      '--idle-timeout-mins 2 '
                                                                      '--max-instances 200 '
                                                                      '--restart-backoff-factor 2.2')
            self.assertEqual(0, cp.returncode, cp.stderr)
            token = util.load_token(self.waiter_url, token_name)
            self.assertFalse(token['https-redirect'])
            self.assertEqual(20, token['fallback-period-secs'])
            self.assertEqual(2, token['idle-timeout-mins'])
            self.assertEqual(200, token['max-instances'])
            self.assertEqual(2.2, token['restart-backoff-factor'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_basic_show(self):
        token_name = self.token_name()
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
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'https-redirect': True, 'fallback-period-secs': 10})
        try:
            cp = cli.show(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Https redirect', cli.stdout(cp))
            self.assertIn('Fallback period (seconds)', cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_show_no_cluster(self):
        config = {'clusters': []}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp = cli.show(token_name=self.token_name(), flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('must specify at least one cluster', cli.decode(cp.stderr))

    def test_show_json(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            cp, tokens = cli.show_token(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(tokens))
            self.assertEqual(util.load_token(self.waiter_url, token_name), tokens[0])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_if_match(self):

        def encountered_stale_token_error(cp):
            self.logger.info(f'Return code: {cp.returncode}, output: {cli.output(cp)}')
            assert 1 == cp.returncode
            assert 'stale token' in cli.decode(cp.stderr)
            return True

        token_name = self.token_name()
        keep_running = True

        def update_token_loop():
            mem = 1
            while keep_running:
                util.post_token(self.waiter_url, token_name, {'mem': mem})
                mem += 1

        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        thread = threading.Thread(target=update_token_loop)
        try:
            thread.start()
            util.wait_until(lambda: cli.create_minimal(self.waiter_url, token_name),
                            encountered_stale_token_error,
                            wait_interval_ms=0)
        finally:
            keep_running = False
            thread.join()
            self.logger.info('Thread finished')
            util.delete_token(self.waiter_url, token_name)

    @unittest.skipIf('WAITER_TEST_CLI_COMMAND' in os.environ, 'waiter executable may be unknown.')
    def test_base_config_file(self):
        token_name = self.token_name()
        cluster_name_1 = str(uuid.uuid4())
        config = {'clusters': [{"name": cluster_name_1, "url": self.waiter_url}]}
        with cli.temp_base_config_file(config):
            # Use entry in base config file
            cp = cli.create_minimal(token_name=token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn(f'on {cluster_name_1} cluster', cli.decode(cp.stdout))

            # Overwrite "base" with specified config file
            cluster_name_2 = str(uuid.uuid4())
            config = {'clusters': [{"name": cluster_name_2, "url": self.waiter_url}]}
            with cli.temp_config_file(config) as path:
                # Verify "base" config is overwritten
                flags = '--config %s' % path
                cp = cli.create_minimal(token_name=token_name, flags=flags)
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertIn(f'on {cluster_name_2} cluster', cli.decode(cp.stdout))
