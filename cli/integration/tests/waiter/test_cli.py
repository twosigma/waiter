import getpass
import json
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

    def test_basic_update(self):
        token_name = self.token_name()
        version = str(uuid.uuid4())
        cmd = util.minimal_service_cmd()
        cp = cli.update_minimal(self.waiter_url, token_name, flags=None, cmd=cmd, cpus=0.1, mem=128, version=version)
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token_data = util.load_token(self.waiter_url, token_name)
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

    def test_create_help_text(self):
        cp = cli.create(create_flags='--help')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('memory (in MiB) to reserve', cli.stdout(cp))

    def test_implicit_update_args(self):
        cp = cli.create(create_flags='--help')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('--cpus', cli.stdout(cp))
        self.assertNotIn('--https-redirect', cli.stdout(cp))
        self.assertNotIn('--fallback-period-secs', cli.stdout(cp))
        self.assertNotIn('--idle-timeout-mins', cli.stdout(cp))
        self.assertNotIn('--max-instances', cli.stdout(cp))
        self.assertNotIn('--restart-backoff-factor', cli.stdout(cp))
        token_name = self.token_name()
        cp = cli.update(self.waiter_url, token_name, create_flags='--https-redirect true '
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
            cp = cli.update(self.waiter_url, token_name, create_flags='--https-redirect false '
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
        self.assertIn('No matching data found', cli.stdout(cp))
        token_definition = {
            'cmd-type': 'shell',
            'health-check-url': '/foo',
            'min-instances': 1,
            'max-instances': 2,
            'permitted-user': '*',
            'mem': 1024
        }
        util.post_token(self.waiter_url, token_name, token_definition)
        try:
            token = util.load_token(self.waiter_url, token_name)
            self.assertIsNotNone(token)
            cp = cli.show(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Command type', cli.stdout(cp))
            self.assertIn('Health check endpoint', cli.stdout(cp))
            self.assertIn('Minimum instances', cli.stdout(cp))
            self.assertIn('Maximum instances', cli.stdout(cp))
            self.assertIn('Permitted user(s)', cli.stdout(cp))
            self.assertIn(f'=== {self.waiter_url} / {token_name} ===', cli.stdout(cp))
            self.assertIn('1 GiB', cli.stdout(cp))
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

    def test_create_if_match(self):

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

    def test_avoid_exit_on_connection_error(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            config = {'clusters': [{'name': 'foo', 'url': self.waiter_url},
                                   {'name': 'bar', 'url': 'http://localhost:65535'}]}
            with cli.temp_config_file(config) as path:
                flags = f'--config {path}'
                cp, tokens = cli.show_token(token_name=token_name, flags=flags)
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertEqual(1, len(tokens), tokens)
                self.assertEqual(util.load_token(self.waiter_url, token_name), tokens[0])
                self.assertIn('Encountered connection error with bar', cli.decode(cp.stderr), cli.output(cp))
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_show_env(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'env': {'FOO': '1', 'BAR': 'baz'}})
        try:
            cp = cli.show(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Environment:\n', cli.stdout(cp))
            self.assertNotIn('Env ', cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_delete_basic(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            cp = cli.delete(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Deleting token', cli.stdout(cp))
            self.assertIn('Successfully deleted', cli.stdout(cp))
            resp_json = util.load_token(self.waiter_url, token_name, expected_status_code=404)
            self.assertIn('waiter-error', resp_json)
        finally:
            util.delete_token(self.waiter_url, token_name, assert_response=False)

    def test_delete_single_service(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            self.logger.info(f'Token: {util.load_token(self.waiter_url, token_name)}')
            service_id = util.ping_token(self.waiter_url, token_name)
            try:
                cp = cli.delete(self.waiter_url, token_name)
                self.assertEqual(1, cp.returncode, cli.output(cp))
                self.assertIn('There is one service using token', cli.stderr(cp))
                self.assertIn('Please kill this service before deleting the token', cli.stderr(cp))
                self.assertIn(service_id, cli.stderr(cp))
            finally:
                util.kill_service(self.waiter_url, service_id)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_delete_multiple_services(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            self.logger.info(f'Token: {util.load_token(self.waiter_url, token_name)}')
            service_id_1 = util.ping_token(self.waiter_url, token_name)
            try:
                util.post_token(self.waiter_url, token_name, util.minimal_service_description())
                self.logger.info(f'Token: {util.load_token(self.waiter_url, token_name)}')
                service_id_2 = util.ping_token(self.waiter_url, token_name)
                try:
                    services_for_token = util.services_for_token(self.waiter_url, token_name)
                    self.logger.info(f'Services for token {token_name}: {json.dumps(services_for_token, indent=2)}')
                    cp = cli.delete(self.waiter_url, token_name)
                    self.assertEqual(1, cp.returncode, cli.output(cp))
                    self.assertIn('There are 2 services using token', cli.stderr(cp))
                    self.assertIn('Please kill these services before deleting the token', cli.stderr(cp))
                    self.assertIn(service_id_1, cli.stderr(cp))
                    self.assertIn(service_id_2, cli.stderr(cp))
                finally:
                    util.kill_service(self.waiter_url, service_id_2)
            finally:
                util.kill_service(self.waiter_url, service_id_1)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_delete_if_match(self):

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
            util.wait_until(lambda: cli.delete(self.waiter_url, token_name),
                            encountered_stale_token_error,
                            wait_interval_ms=0)
        finally:
            keep_running = False
            thread.join()
            self.logger.info('Thread finished')
            util.delete_token(self.waiter_url, token_name)

    def test_delete_non_existent_token(self):
        token_name = self.token_name()
        cp = cli.delete(self.waiter_url, token_name)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('No matching data found', cli.stdout(cp))

    def test_ping_basic(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.ping(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Pinging token', cli.stdout(cp))
            self.assertIn('successful', cli.stdout(cp))
            self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_ping_error(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.ping(self.waiter_url, token_name)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Pinging token', cli.stdout(cp))
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_ping_non_existent_token(self):
        token_name = self.token_name()
        cp = cli.ping(self.waiter_url, token_name)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('No matching data found', cli.stdout(cp))

    def test_ping_custom_health_check_endpoint(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description(**{'health-check-url': '/sleep'}))
        try:
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.ping(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Pinging token', cli.stdout(cp))
            self.assertIn('/sleep', cli.stdout(cp))
            self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_kill_basic(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            service_id = util.ping_token(self.waiter_url, token_name)
            self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.kill(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Killing service', cli.stdout(cp))
            self.assertIn(service_id, cli.stdout(cp))
            self.assertIn('Successfully killed', cli.stdout(cp))
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_kill_no_services(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            cp = cli.kill(self.waiter_url, token_name)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('There are no services using token', cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_kill_multiple_services(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            service_id_1 = util.ping_token(self.waiter_url, token_name)
            util.post_token(self.waiter_url, token_name, util.minimal_service_description())
            service_id_2 = util.ping_token(self.waiter_url, token_name)
            self.assertEqual(2, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.kill(self.waiter_url, token_name, kill_flags='--force')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('There are 2 services using token', cli.stdout(cp))
            self.assertEqual(2, cli.stdout(cp).count('Killing service'))
            self.assertEqual(2, cli.stdout(cp).count('Successfully killed'))
            self.assertIn(service_id_1, cli.stdout(cp))
            self.assertIn(service_id_2, cli.stdout(cp))
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_kill_timeout(self):
        token_name = self.token_name()

        def ping_then_kill_with_small_timeout():
            util.ping_token(self.waiter_url, token_name)
            assert 1 == len(util.services_for_token(self.waiter_url, token_name))
            return cli.kill(self.waiter_url, token_name, kill_flags='--timeout 1')

        def kill_timed_out(cp):
            self.logger.info(f'Return code: {cp.returncode}')
            assert 1 == cp.returncode
            assert 'Timeout waiting for service to die' in cli.stderr(cp)
            return True

        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            util.wait_until(ping_then_kill_with_small_timeout, kill_timed_out)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_kill_services_sorted(self):
        token_name = self.token_name()
        service_description_1 = util.minimal_service_description()
        util.post_token(self.waiter_url, token_name, service_description_1)
        try:
            # Create two services for the token
            service_id_1 = util.ping_token(self.waiter_url, token_name)
            service_description_2 = util.minimal_service_description()
            util.post_token(self.waiter_url, token_name, service_description_2)
            service_id_2 = util.ping_token(self.waiter_url, token_name)

            # Kill the two services and assert the sort order
            cp = cli.kill(self.waiter_url, token_name, kill_flags='--force')
            stdout = cli.stdout(cp)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn(service_id_1, stdout)
            self.assertIn(service_id_2, stdout)
            self.assertLess(stdout.index(service_id_2), stdout.index(service_id_1))

            # Re-create the same two services, in the opposite order
            util.post_token(self.waiter_url, token_name, service_description_2)
            util.ping_token(self.waiter_url, token_name)
            util.post_token(self.waiter_url, token_name, service_description_1)
            util.ping_token(self.waiter_url, token_name)

            # Kill the two services and assert the (different) sort order
            cp = cli.kill(self.waiter_url, token_name, kill_flags='--force')
            stdout = cli.stdout(cp)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn(service_id_1, stdout)
            self.assertIn(service_id_2, stdout)
            self.assertLess(stdout.index(service_id_1), stdout.index(service_id_2))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)
            
    def test_ping_timeout(self):
        token_name = self.token_name()
        command = f'{util.default_cmd()} --start-up-sleep-ms 20000'
        util.post_token(self.waiter_url, token_name, util.minimal_service_description(cmd=command))
        try:
            cp = cli.ping(self.waiter_url, token_name, ping_flags='--timeout 300')
            self.assertEqual(0, cp.returncode, cp.stderr)
            util.kill_services_using_token(self.waiter_url, token_name)
            cp = cli.ping(self.waiter_url, token_name, ping_flags='--timeout 10')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Encountered error', cli.stderr(cp))
        finally:
            util.kill_services_using_token(self.waiter_url, token_name)
            util.delete_token(self.waiter_url, token_name)

    def test_ping_service_id(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            service_id = util.ping_token(self.waiter_url, token_name)
            util.kill_services_using_token(self.waiter_url, token_name)
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.ping(self.waiter_url, service_id, ping_flags='--service-id')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Pinging service', cli.stdout(cp))
            self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_ping_invalid_args(self):
        cp = cli.ping(self.waiter_url)
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('the following arguments are required: token-or-service-id', cli.stderr(cp))

    def test_ping_correct_endpoint(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name,
                        util.minimal_service_description(**{'health-check-url': '/sleep'}))
        try:
            # Grab the service id for the /sleep version
            service_id = util.ping_token(self.waiter_url, token_name)

            # Update the health check url to /status
            util.post_token(self.waiter_url, token_name,
                            util.minimal_service_description(**{'health-check-url': '/status'}))

            # Pinging the token should use /status
            cp = cli.ping(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('/status', cli.stdout(cp))

            # Pinging the service id should use /sleep
            cp = cli.ping(self.waiter_url, service_id, ping_flags='--service-id')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('/sleep', cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)
