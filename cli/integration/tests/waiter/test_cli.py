import datetime
import getpass
import json
import logging
import os
import re
import tempfile
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
            self.assertIn('Attempting to create', cli.stdout(cp))
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
            self.assertIn('Attempting to update', cli.stdout(cp))
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
        self.assertNotIn('--health-check-port-index', cli.stdout(cp))
        self.assertNotIn('--concurrency-level', cli.stdout(cp))
        self.assertNotIn('--health-check-max-consecutive-failures', cli.stdout(cp))
        self.assertNotIn('--max-queue-length', cli.stdout(cp))
        self.assertNotIn('--expired-instance-restart-rate', cli.stdout(cp))
        self.assertNotIn('--jitter-threshold', cli.stdout(cp))
        token_name = self.token_name()
        cp = cli.create(self.waiter_url, token_name, create_flags=('--https-redirect true '
                                                                   '--cpus 0.1 '
                                                                   '--fallback-period-secs 10 '
                                                                   '--idle-timeout-mins 1 '
                                                                   '--max-instances 100 '
                                                                   '--restart-backoff-factor 1.1 '
                                                                   '--health-check-port-index 1 '
                                                                   '--concurrency-level 1000 '
                                                                   '--health-check-max-consecutive-failures 10 '
                                                                   '--max-queue-length 1000000 '
                                                                   '--expired-instance-restart-rate 0.1 '
                                                                   '--jitter-threshold 0.1 '))
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token = util.load_token(self.waiter_url, token_name)
            self.assertTrue(token['https-redirect'])
            self.assertEqual(10, token['fallback-period-secs'])
            self.assertEqual(1, token['idle-timeout-mins'])
            self.assertEqual(100, token['max-instances'])
            self.assertEqual(1.1, token['restart-backoff-factor'])
            self.assertEqual(1, token['health-check-port-index'])
            self.assertEqual(1000, token['concurrency-level'])
            self.assertEqual(10, token['health-check-max-consecutive-failures'])
            self.assertEqual(1000000, token['max-queue-length'])
            self.assertEqual(0.1, token['expired-instance-restart-rate'])
            self.assertEqual(0.1, token['jitter-threshold'])
            cp = cli.create(self.waiter_url, token_name, create_flags=('--https-redirect false '
                                                                       '--cpus 0.1 '
                                                                       '--fallback-period-secs 20 '
                                                                       '--idle-timeout-mins 2 '
                                                                       '--max-instances 200 '
                                                                       '--restart-backoff-factor 2.2 '
                                                                       '--health-check-port-index 2 '
                                                                       '--concurrency-level 2000 '
                                                                       '--health-check-max-consecutive-failures 2 '
                                                                       '--max-queue-length 2000000 '
                                                                       '--expired-instance-restart-rate 0.2 '
                                                                       '--jitter-threshold 0.2 '))
            self.assertEqual(0, cp.returncode, cp.stderr)
            token = util.load_token(self.waiter_url, token_name)
            self.assertFalse(token['https-redirect'])
            self.assertEqual(20, token['fallback-period-secs'])
            self.assertEqual(2, token['idle-timeout-mins'])
            self.assertEqual(200, token['max-instances'])
            self.assertEqual(2.2, token['restart-backoff-factor'])
            self.assertEqual(2, token['health-check-port-index'])
            self.assertEqual(2000, token['concurrency-level'])
            self.assertEqual(2, token['health-check-max-consecutive-failures'])
            self.assertEqual(2000000, token['max-queue-length'])
            self.assertEqual(0.2, token['expired-instance-restart-rate'])
            self.assertEqual(0.2, token['jitter-threshold'])
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
        cp = cli.update(self.waiter_url, token_name, update_flags='--https-redirect true '
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
            cp = cli.update(self.waiter_url, token_name, update_flags='--https-redirect false '
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

    @pytest.mark.serial
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
                util.post_token(self.waiter_url, token_name, {'mem': mem}, assert_response=False)
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
            try:
                self.assertIn(f'on {cluster_name_1}', cli.decode(cp.stdout))

                # Overwrite "base" with specified config file
                cluster_name_2 = str(uuid.uuid4())
                config = {'clusters': [{"name": cluster_name_2, "url": self.waiter_url}]}
                with cli.temp_config_file(config) as path:
                    # Verify "base" config is overwritten
                    flags = '--config %s' % path
                    cp = cli.create_minimal(token_name=token_name, flags=flags)
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn(f'on {cluster_name_2}', cli.decode(cp.stdout))
            finally:
                util.delete_token(self.waiter_url, token_name)

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

    @pytest.mark.serial
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
                util.post_token(self.waiter_url, token_name, {'mem': mem}, assert_response=False)
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
            util.delete_token(self.waiter_url, token_name, assert_response=False)

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
            self.assertIn('Service is currently', cli.stdout(cp))
            self.assertTrue(any(s in cli.stdout(cp) for s in ['Running', 'Starting']))
            util.wait_until_services_for_token(self.waiter_url, token_name, 1)
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
            util.wait_until_no_services_for_token(self.waiter_url, token_name)
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
            util.wait_until_no_services_for_token(self.waiter_url, token_name)
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_kill_timeout(self):
        token_name = self.token_name()

        def ping_then_kill_with_small_timeout():
            util.post_token(self.waiter_url, token_name, util.minimal_service_description())
            util.ping_token(self.waiter_url, token_name)
            assert 1 == len(util.services_for_token(self.waiter_url, token_name))
            return cli.kill(self.waiter_url, token_name, kill_flags='--timeout 1')

        def kill_timed_out(cp):
            self.logger.info(f'Return code: {cp.returncode}')
            assert 1 == cp.returncode
            assert 'Timeout waiting for service to die' in cli.stderr(cp)
            return True

        try:
            util.wait_until(ping_then_kill_with_small_timeout, kill_timed_out)
        finally:
            util.delete_token(self.waiter_url, token_name)

    @pytest.mark.xfail
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
            util.wait_until_routers_recognize_service_killed(self.waiter_url, service_id_1)
            util.wait_until_routers_recognize_service_killed(self.waiter_url, service_id_2)

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
            self.assertTrue(
                # Either Waiter will inform us that the ping timed out
                'Ping request timed out' in cli.stderr(cp) or
                # Or, the read from Waiter will time out
                'Encountered error while pinging' in cli.stderr(cp))
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

            # Pinging the service id should use /sleep
            cp = cli.ping(self.waiter_url, service_id, ping_flags='--service-id')
            self.assertEqual(0, cp.returncode, cp.stderr)
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_ping_no_wait(self):
        token_name = self.token_name()
        command = f'{util.default_cmd()} --start-up-sleep-ms {util.DEFAULT_TEST_TIMEOUT_SECS * 2 * 1000}'
        util.post_token(self.waiter_url, token_name, util.minimal_service_description(cmd=command))
        try:
            cp = cli.ping(self.waiter_url, token_name, ping_flags='--no-wait')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Service is currently Starting', cli.stdout(cp))
            services_for_token = util.wait_until_services_for_token(self.waiter_url, token_name, 1)

            service_id = services_for_token[0]['service-id']
            util.kill_services_using_token(self.waiter_url, token_name)
            cp = cli.ping(self.waiter_url, service_id, ping_flags='--service-id --no-wait')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Service is currently Starting', cli.stdout(cp))
            util.wait_until_services_for_token(self.waiter_url, token_name, 1)

            util.kill_services_using_token(self.waiter_url, token_name)
            util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'cmd-type': 'shell'})
            cp = cli.ping(self.waiter_url, token_name, ping_flags='--no-wait')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertNotIn('Service is currently', cli.stdout(cp))
            self.assertIn('Service description', cli.decode(cp.stderr))
            self.assertIn('improper', cli.decode(cp.stderr))
            self.assertIn('cmd must be a non-empty string', cli.decode(cp.stderr))
            self.assertIn('version must be a non-empty string', cli.decode(cp.stderr))
            self.assertIn('mem must be a positive number', cli.decode(cp.stderr))
            util.wait_until_no_services_for_token(self.waiter_url, token_name)
        finally:
            util.kill_services_using_token(self.waiter_url, token_name)
            util.delete_token(self.waiter_url, token_name)

    def test_create_does_not_patch(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            cp = cli.create_from_service_description(self.waiter_url, token_name, {'mem': 128})
            self.assertEqual(0, cp.returncode, cp.stderr)
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertFalse('cpus' in token_data)
            self.assertEqual(128, token_data['mem'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_update_does_patch(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            cp = cli.update_from_service_description(self.waiter_url, token_name, {'mem': 128})
            self.assertEqual(0, cp.returncode, cp.stderr)
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual(0.1, token_data['cpus'])
            self.assertEqual(128, token_data['mem'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_create_token_json(self):
        create_fields = {'cpus': 0.1, 'mem': 128}

        # Test with json from stdin
        token_name = self.token_name()
        stdin = json.dumps(create_fields).encode('utf8')
        cp = cli.create(self.waiter_url, token_name, create_flags=f'--json -', stdin=stdin)
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual(0.1, token_data['cpus'])
            self.assertEqual(128, token_data['mem'])

            # Test with json from a file
            util.delete_token(self.waiter_url, token_name)
            with cli.temp_token_file(create_fields) as path:
                cp = cli.create(self.waiter_url, token_name, create_flags=f'--json {path}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.1, token_data['cpus'])
                self.assertEqual(128, token_data['mem'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_update_token_json(self):
        token_name = self.token_name()
        create_fields = {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'}
        update_fields = {'cpus': 0.2, 'mem': 256}
        util.post_token(self.waiter_url, token_name, create_fields)
        try:
            # Test with json from stdin
            stdin = json.dumps(update_fields).encode('utf8')
            cp = cli.update(self.waiter_url, token_name, update_flags=f'--json -', stdin=stdin)
            self.assertEqual(0, cp.returncode, cp.stderr)
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual(0.2, token_data['cpus'])
            self.assertEqual(256, token_data['mem'])
            self.assertEqual('foo', token_data['cmd'])

            # Test with json from a file
            util.post_token(self.waiter_url, token_name, create_fields)
            with cli.temp_token_file(update_fields) as path:
                cp = cli.update(self.waiter_url, token_name, update_flags=f'--json {path}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.2, token_data['cpus'])
                self.assertEqual(256, token_data['mem'])
                self.assertEqual('foo', token_data['cmd'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_post_token_json_and_flags(self):
        token_name = self.token_name()
        update_fields = {'cpus': 0.2, 'mem': 256}
        with cli.temp_token_file(update_fields) as path:
            cp = cli.update(self.waiter_url, token_name, update_flags=f'--json {path} --cpus 0.1')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the same parameter in both a token JSON file and token field flags at the '
                          'same time (cpus)', cli.stderr(cp))

            cp = cli.update(self.waiter_url, token_name, update_flags=f'--json {path} --mem 128')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the same parameter in both a token JSON file and token field flags at the '
                          'same time (mem)', cli.stderr(cp))

            cp = cli.update(self.waiter_url, token_name, update_flags=f'--json {path} --cpus 0.1 --mem 128')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the same parameter in both a token JSON file and token field flags',
                          cli.stderr(cp))
            self.assertIn('cpus', cli.stderr(cp))
            self.assertIn('mem', cli.stderr(cp))

            try:
                cp = cli.update(self.waiter_url, token_name, update_flags=f'--json {path} --name foo --image bar')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.2, token_data['cpus'])
                self.assertEqual(256, token_data['mem'])
                self.assertEqual('foo', token_data['name'])
                self.assertEqual('bar', token_data['image'])
            finally:
                util.delete_token(self.waiter_url, token_name)

    def test_post_token_json_invalid(self):
        token_name = self.token_name()

        stdin = json.dumps([]).encode('utf8')
        cp = cli.update(self.waiter_url, token_name, update_flags=f'--json -', stdin=stdin)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Token must be a dictionary', cli.stderr(cp))

        stdin = '{"mem": 128'.encode('utf8')
        cp = cli.update(self.waiter_url, token_name, update_flags=f'--json -', stdin=stdin)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Malformed JSON', cli.stderr(cp))

        with tempfile.NamedTemporaryFile(delete=True) as file:
            cp = cli.update(self.waiter_url, token_name, update_flags=f'--json {file.name}')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Unable to load token JSON from', cli.stderr(cp))

    def test_kill_service_id(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            service_id = util.ping_token(self.waiter_url, token_name)
            self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.kill(self.waiter_url, service_id, kill_flags='--service-id')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Killing service', cli.stdout(cp))
            util.wait_until_no_services_for_token(self.waiter_url, token_name)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_kill_bogus_service_id(self):
        cp = cli.kill(self.waiter_url, uuid.uuid4(), kill_flags='--service-id')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('No matching data found', cli.stdout(cp))

    def test_kill_inactive_service_id(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            service_id = util.ping_token(self.waiter_url, token_name)
            util.kill_services_using_token(self.waiter_url, token_name)
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.kill(self.waiter_url, service_id, kill_flags='--service-id')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('cannot be killed because it is already Inactive', cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_init_basic(self):
        token_name = self.token_name()
        filename = str(uuid.uuid4())
        flags = f"--cmd '{util.default_cmd()}' --file {filename} --cmd-type shell --health-check-url /status"
        cp = cli.init(self.waiter_url, init_flags=flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('Writing token JSON', cli.stdout(cp))
        try:
            token_definition = util.load_json_file(filename)
            self.logger.info(f'Token definition: {json.dumps(token_definition, indent=2)}')
            util.post_token(self.waiter_url, token_name, token_definition)
            try:
                token = util.load_token(self.waiter_url, token_name)
                self.assertEqual('your-app-name', token['name'])
                self.assertEqual('your-metric-group', token['metric-group'])
                self.assertEqual('shell', token['cmd-type'])
                self.assertEqual(util.default_cmd(), token['cmd'])
                self.assertEqual('your version', token['version'])
                self.assertEqual(0.1, token['cpus'])
                self.assertEqual(2048, token['mem'])
                self.assertEqual('/status', token['health-check-url'])
                self.assertEqual(120, token['concurrency-level'])
                self.assertEqual('*', token['permitted-user'])
                self.assertEqual(getpass.getuser(), token['run-as-user'])
                util.ping_token(self.waiter_url, token_name)
                self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
            finally:
                util.delete_token(self.waiter_url, token_name, kill_services=True)
        finally:
            os.remove(filename)

    def test_implicit_init_args(self):
        cp = cli.init(init_flags='--help')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn('--cpus', cli.stdout(cp))
        self.assertNotIn('--https-redirect', cli.stdout(cp))
        self.assertNotIn('--fallback-period-secs', cli.stdout(cp))
        self.assertNotIn('--idle-timeout-mins', cli.stdout(cp))
        self.assertNotIn('--max-instances', cli.stdout(cp))
        self.assertNotIn('--restart-backoff-factor', cli.stdout(cp))
        token_name = self.token_name()
        with tempfile.NamedTemporaryFile(delete=True) as file:
            init_flags = (
                '--cmd-type shell '
                '--https-redirect true '
                '--cpus 0.1 '
                '--fallback-period-secs 10 '
                '--idle-timeout-mins 1 '
                '--max-instances 100 '
                '--restart-backoff-factor 1.1 '
                f'--file {file.name} '
                '--force')
            cp = cli.init(self.waiter_url, init_flags=init_flags)
            self.assertEqual(0, cp.returncode, cp.stderr)
            token_definition = util.load_json_file(file.name)
            self.logger.info(f'Token definition: {json.dumps(token_definition, indent=2)}')
            util.post_token(self.waiter_url, token_name, token_definition)
            try:
                token = util.load_token(self.waiter_url, token_name)
                self.assertEqual('your command', token['cmd'])
                self.assertEqual('shell', token['cmd-type'])
                self.assertEqual('your version', token['version'])
                self.assertEqual(0.1, token['cpus'])
                self.assertEqual(2048, token['mem'])
                self.assertTrue(token['https-redirect'])
                self.assertEqual(10, token['fallback-period-secs'])
                self.assertEqual(1, token['idle-timeout-mins'])
                self.assertEqual(100, token['max-instances'])
                self.assertEqual(1.1, token['restart-backoff-factor'])
            finally:
                util.delete_token(self.waiter_url, token_name)

    def test_init_existing_file(self):
        with tempfile.NamedTemporaryFile(delete=True) as file:
            self.assertTrue(os.path.isfile(file.name))
            cp = cli.init(self.waiter_url, init_flags=f"--cmd '{util.default_cmd()}' --file {file.name}")
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('There is already a file', cli.stderr(cp))
            cp = cli.init(self.waiter_url, init_flags=f"--cmd '{util.default_cmd()}' --file {file.name} --force")
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Writing token JSON', cli.stdout(cp))

    def test_show_services_using_token(self):
        token_name = self.token_name()
        custom_fields = {
            'permitted-user': getpass.getuser(),
            'run-as-user': getpass.getuser(),
            'cpus': 0.1,
            'mem': 128
        }
        service_description_1 = util.minimal_service_description(**custom_fields)
        util.post_token(self.waiter_url, token_name, service_description_1)
        try:
            # Create 2 services, 1 running and 1 failing due to a bad command
            service_id_1 = util.ping_token(self.waiter_url, token_name)
            custom_fields['cmd'] = 'exit 1'
            custom_fields['cpus'] = 0.2
            custom_fields['mem'] = 256
            service_description_2 = util.minimal_service_description(**custom_fields)
            util.post_token(self.waiter_url, token_name, service_description_2)
            service_id_2 = util.ping_token(self.waiter_url, token_name, expected_status_code=503)

            # Run show with --json
            cp, services = cli.show_token_services(self.waiter_url, token_name=token_name)
            self.logger.info(f'Services: {json.dumps(services, indent=2)}')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(2, len(services), services)
            service_1 = next(s for s in services if s['service-id'] == service_id_1)
            service_2 = next(s for s in services if s['service-id'] == service_id_2)
            self.assertEqual(service_description_1, service_1['service-description'])
            self.assertEqual(service_description_2, service_2['service-description'])
            self.assertEqual('Running', service_1['status'])
            self.assertIn(service_2['status'], ['Failing', 'Starting'])

            # Run show without --json
            cp = cli.show(self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIsNotNone(re.search('^# Services\\s+2$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search('^# Failing\\s+([01])$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search('^Total Memory\\s+384 MiB$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search('^Total CPUs\\s+0\\.3$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_1}.+Running.+Not Current$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_2}.+(Failing|Starting).+Current$',
                                           cli.stdout(cp), re.MULTILINE))

            # Run show without --json and with --no-services
            cp = cli.show(self.waiter_url, token_name, show_flags='--no-services')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertNotIn('# Services', cli.stdout(cp))
            self.assertNotIn('# Failing', cli.stdout(cp))
            self.assertNotIn('Total Memory', cli.stdout(cp))
            self.assertNotIn('Total CPUs', cli.stdout(cp))
            self.assertNotIn(service_id_1, cli.stdout(cp))
            self.assertNotIn(service_id_2, cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_tokens_basic(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            # Ensure that tokens lists our token
            cp, tokens = cli.tokens_data(self.waiter_url)
            token_data = next(t for t in tokens if t['token'] == token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertFalse(token_data['deleted'])

            # Delete the token
            util.delete_token(self.waiter_url, token_name)

            # Ensure that tokens does not list our token
            cp, tokens = cli.tokens_data(self.waiter_url)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertFalse(any(t['token'] == token_name for t in tokens))
        finally:
            util.delete_token(self.waiter_url, token_name, assert_response=False)

    def test_tokens_sorted(self):
        token_name_prefix = self.token_name()
        token_name_1 = f'{token_name_prefix}_foo'
        util.post_token(self.waiter_url, token_name_1, util.minimal_service_description())
        try:
            token_name_2 = f'{token_name_prefix}_bar'
            util.post_token(self.waiter_url, token_name_2, util.minimal_service_description())
            try:
                cp = cli.tokens(self.waiter_url)
                stdout = cli.stdout(cp)
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertIn(token_name_1, stdout)
                self.assertIn(token_name_2, stdout)
                self.assertLess(stdout.index(token_name_2), stdout.index(token_name_1))
            finally:
                util.delete_token(self.waiter_url, token_name_2)
        finally:
            util.delete_token(self.waiter_url, token_name_1)

    def test_create_token_json_containing_token_name(self):
        token_name = self.token_name()
        with cli.temp_token_file({'token': token_name, 'cpus': 0.1, 'mem': 128}) as path:
            cp = cli.create(self.waiter_url, create_flags=f'--json {path}')
            self.assertEqual(0, cp.returncode, cp.stderr)
            try:
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.1, token_data['cpus'])
                self.assertEqual(128, token_data['mem'])
            finally:
                util.delete_token(self.waiter_url, token_name)

    def test_update_token_json_containing_token_name(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'})
        try:
            with cli.temp_token_file({'token': token_name, 'cpus': 0.2, 'mem': 256}) as path:
                cp = cli.update(self.waiter_url, update_flags=f'--json {path}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.2, token_data['cpus'])
                self.assertEqual(256, token_data['mem'])
                self.assertEqual('foo', token_data['cmd'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_post_token_over_specified_token_name(self):
        token_name = self.token_name()
        with cli.temp_token_file({'token': token_name}) as path:
            cp = cli.create(self.waiter_url, token_name, create_flags=f'--json {path}')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the token name both as an argument and in the token JSON', cli.stderr(cp))

    def test_post_token_no_token_name(self):
        cp = cli.create(self.waiter_url)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('must specify the token name', cli.stderr(cp))
        with cli.temp_token_file({'cpus': 0.1}) as path:
            cp = cli.create(self.waiter_url, create_flags=f'--json {path}')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('must specify the token name', cli.stderr(cp))

    def test_implicit_args_lenient_parsing(self):
        token_name = self.token_name()
        cp = cli.create(self.waiter_url, token_name, create_flags='--cpus 0.1 --foo-level HIGH --bar-rate LOW')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('Unsupported key(s)', cli.stderr(cp))
        self.assertIn('foo-level', cli.stderr(cp))
        self.assertIn('bar-rate', cli.stderr(cp))

    def test_show_service_current(self):
        token_name_1 = self.token_name()
        token_name_2 = self.token_name()
        custom_fields = {
            'owner': getpass.getuser(),
            'cluster': 'test_show_service_current',
            'root': 'test_show_service_current',
            'last-update-user': getpass.getuser(),
            'last-update-time': datetime.datetime.utcnow().isoformat()
        }
        token_definition = util.minimal_service_description(**custom_fields)

        # Post two identical tokens with different names
        util.post_token(self.waiter_url, token_name_1, token_definition, update_mode_admin=True, assert_response=False)
        util.post_token(self.waiter_url, token_name_2, token_definition, update_mode_admin=True, assert_response=False)
        try:
            # Assert that their etags match
            etag_1 = util.load_token_with_headers(self.waiter_url, token_name_1)[1]['ETag']
            etag_2 = util.load_token_with_headers(self.waiter_url, token_name_2)[1]['ETag']
            self.assertEqual(etag_1, etag_2)

            # Create service A from the two tokens
            service_id_a = util.ping_token(self.waiter_url, f'{token_name_1},{token_name_2}')

            # Update token #2 only and assert that their etags don't match
            token_definition['cpus'] += 0.1
            util.post_token(self.waiter_url, token_name_2, token_definition, update_mode_admin=True,
                            assert_response=False, etag=etag_1)
            etag_1 = util.load_token_with_headers(self.waiter_url, token_name_1)[1]['ETag']
            etag_2 = util.load_token_with_headers(self.waiter_url, token_name_2)[1]['ETag']
            self.assertNotEqual(etag_1, etag_2)

            # Create service B from the two tokens
            service_id_b = util.ping_token(self.waiter_url, f'{token_name_1},{token_name_2}')

            # Update token #1 to match token #2 and assert that their etags match
            util.post_token(self.waiter_url, token_name_1, token_definition, update_mode_admin=True,
                            assert_response=False, etag=etag_1)
            etag_1 = util.load_token_with_headers(self.waiter_url, token_name_1)[1]['ETag']
            etag_2 = util.load_token_with_headers(self.waiter_url, token_name_2)[1]['ETag']
            self.assertEqual(etag_1, etag_2)

            # Update token #2 only and assert that their etags don't match
            token_definition['cpus'] += 0.1
            util.post_token(self.waiter_url, token_name_2, token_definition, update_mode_admin=True,
                            assert_response=False, etag=etag_1)
            etag_1 = util.load_token_with_headers(self.waiter_url, token_name_1)[1]['ETag']
            etag_2 = util.load_token_with_headers(self.waiter_url, token_name_2)[1]['ETag']
            self.assertNotEqual(etag_1, etag_2)

            # Create service C from the two tokens
            service_id_c = util.ping_token(self.waiter_url, f'{token_name_1},{token_name_2}')

            # For both tokens, only service C should be "current"
            cp = cli.show(self.waiter_url, token_name_1)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIsNotNone(re.search(f'^{service_id_a}.+Not Current$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_b}.+Not Current$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_c}.+Current$', cli.stdout(cp), re.MULTILINE))
            cp = cli.show(self.waiter_url, token_name_2)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIsNotNone(re.search(f'^{service_id_a}.+Not Current$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_b}.+Not Current$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_c}.+Current$', cli.stdout(cp), re.MULTILINE))
        finally:
            util.delete_token(self.waiter_url, token_name_1, kill_services=True)
            util.delete_token(self.waiter_url, token_name_2, kill_services=True)
