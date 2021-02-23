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
from functools import partial
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

    def __test_no_cluster(self, cli_fn):
        config = {'clusters': []}
        with cli.temp_config_file(config) as path:
            flags = '--config %s' % path
            cp = cli_fn(token_name=self.token_name(), flags=flags)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('must specify at least one cluster', cli.decode(cp.stderr))

    def test_create_no_cluster(self):
        self.__test_no_cluster(cli.create_minimal)

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

    def test_cli_invalid_file_format_combo(self):
        cp = cli.create(self.waiter_url, create_flags='--json test.json --yaml test.yaml')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('not allowed with argument', cli.stderr(cp))

        cp = cli.update(self.waiter_url, update_flags='--json test.json --yaml test.yaml')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('not allowed with argument', cli.stderr(cp))

        token_name = self.token_name()
        cp = cli.show(self.waiter_url, token_name, show_flags='--json --yaml')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('not allowed with argument', cli.stderr(cp))

        token_name = self.token_name()
        cp = cli.show(self.waiter_url, token_name, show_flags='--json --yaml')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('not allowed with argument', cli.stderr(cp))

        cp = cli.tokens(self.waiter_url, tokens_flags='--json --yaml')
        self.assertEqual(2, cp.returncode, cp.stderr)
        self.assertIn('not allowed with argument', cli.stderr(cp))

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

    def __test_show(self, file_format):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1})
        try:
            cp, tokens = cli.show_token(file_format, self.waiter_url, token_name)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(1, len(tokens))
            self.assertEqual(util.load_token(self.waiter_url, token_name), tokens[0])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_show_json(self):
        self.__test_show('json')

    def test_show_yaml(self):
        self.__test_show('yaml')

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
                cp, tokens = cli.show_token('json', token_name=token_name, flags=flags)
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
            cp = cli.kill(self.waiter_url, token_name, flags="-v")
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Killing service', cli.stdout(cp))
            self.assertIn(service_id, cli.stdout(cp))
            self.assertIn('Successfully killed', cli.stdout(cp))
            self.assertIn('timeout=30000', cli.stderr(cp))
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

    def test_kill_timeout(self):
        timeout = 10
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description())
        try:
            service_id = util.ping_token(self.waiter_url, token_name)
            self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.kill(self.waiter_url, token_name, flags="-v", kill_flags=f"--timeout {timeout}")
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn('Killing service', cli.stdout(cp))
            self.assertIn(service_id, cli.stdout(cp))
            self.assertIn('Successfully killed', cli.stdout(cp))
            self.assertIn(f'timeout={timeout * 1000}', cli.stderr(cp))
            util.wait_until_no_services_for_token(self.waiter_url, token_name)
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

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

    def test_ping_deployment_errors(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description(**{'cmd': 'asdfasdfafsdhINVALIDCOMMAND'}))
        try:
            self.assertEqual(0, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.ping(self.waiter_url, token_name)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Pinging token', cli.stdout(cp))
            self.assertIn('Ping responded with non-200 status 503.', cli.stderr(cp))
            self.assertIn('Deployment error: Invalid startup command', cli.stderr(cp))
            self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

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

    def __test_create_token(self, file_format, input_flag=None):
        if input_flag is None:
            input_flag = file_format

        create_fields = {'cpus': 0.1, 'mem': 128}
        stdin = cli.dump(file_format, create_fields)

        token_name = self.token_name()
        cp = cli.create(self.waiter_url, token_name, create_flags=f'--{input_flag} -', stdin=stdin)
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual(0.1, token_data['cpus'])
            self.assertEqual(128, token_data['mem'])

            # Test with data from a file
            util.delete_token(self.waiter_url, token_name)
            with cli.temp_token_file(create_fields, file_format) as path:
                cp = cli.create(self.waiter_url, token_name, create_flags=f'--{input_flag} {path}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.1, token_data['cpus'])
                self.assertEqual(128, token_data['mem'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_create_token_json(self):
        self.__test_create_token('json')

    def test_create_token_yaml(self):
        self.__test_create_token('yaml')

    def test_create_token_json_input(self):
        self.__test_create_token('json', 'input')

    def test_create_token_yaml_input(self):
        self.__test_create_token('yaml', 'input')

    def __test_update_token(self, file_format):
        token_name = self.token_name()
        create_fields = {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'}
        update_fields = {'cpus': 0.2, 'mem': 256}
        util.post_token(self.waiter_url, token_name, create_fields)
        try:
            stdin = cli.dump(file_format, update_fields)

            cp = cli.update(self.waiter_url, token_name, update_flags=f'--{file_format} -', stdin=stdin)
            self.assertEqual(0, cp.returncode, cp.stderr)
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual(0.2, token_data['cpus'])
            self.assertEqual(256, token_data['mem'])
            self.assertEqual('foo', token_data['cmd'])

            # Test with data from a file
            util.post_token(self.waiter_url, token_name, create_fields)
            with cli.temp_token_file(update_fields, file_format) as path:
                cp = cli.update(self.waiter_url, token_name, update_flags=f'--{file_format} {path}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.2, token_data['cpus'])
                self.assertEqual(256, token_data['mem'])
                self.assertEqual('foo', token_data['cmd'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_update_token_json(self):
        self.__test_update_token('json')

    def test_update_token_yaml(self):
        self.__test_update_token('yaml')

    def __test_post_token_and_flags(self, file_format):
        token_name = self.token_name()
        update_fields = {'cpus': 0.2, 'mem': 256}
        with cli.temp_token_file(update_fields, file_format) as path:
            cp = cli.update(self.waiter_url, token_name,
                            update_flags=f'--{file_format} {path} --cpus 0.1')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the same parameter in both an input file and token field flags at the '
                          'same time (cpus)', cli.stderr(cp))

            cp = cli.update(self.waiter_url, token_name,
                            update_flags=f'--{file_format} {path} --mem 128')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the same parameter in both an input file and token field flags at the '
                          'same time (mem)', cli.stderr(cp))

            cp = cli.update(self.waiter_url, token_name,
                            update_flags=f'--{file_format} {path} --cpus 0.1 --mem 128')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the same parameter in both an input file and token field flags',
                          cli.stderr(cp))
            self.assertIn('cpus', cli.stderr(cp))
            self.assertIn('mem', cli.stderr(cp))

            try:
                cp = cli.update(self.waiter_url, token_name,
                                update_flags=f'--{file_format} {path} --name foo --image bar')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.2, token_data['cpus'])
                self.assertEqual(256, token_data['mem'])
                self.assertEqual('foo', token_data['name'])
                self.assertEqual('bar', token_data['image'])
            finally:
                util.delete_token(self.waiter_url, token_name)

    def test_post_token_json_and_flags(self):
        self.__test_post_token_and_flags('json')

    def test_post_token_yaml_and_flags(self):
        self.__test_post_token_and_flags('yaml')

    def __test_post_token_invalid(self, file_format):
        token_name = self.token_name()

        stdin = json.dumps([]).encode('utf8')
        cp = cli.update(self.waiter_url, token_name, update_flags=f'--{file_format} -', stdin=stdin)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(f'Input {file_format.upper()} must be a dictionary', cli.stderr(cp))

        stdin = '{"mem": 128'.encode('utf8')
        cp = cli.update(self.waiter_url, token_name, update_flags=f'--{file_format} -', stdin=stdin)
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn(f'Malformed {file_format.upper()}', cli.stderr(cp))

        with tempfile.NamedTemporaryFile(delete=True) as file:
            cp = cli.update(self.waiter_url, token_name, update_flags=f'--{file_format} {file.name}')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn(f'Unable to load {file_format.upper()} from', cli.stderr(cp))

    def test_post_token_json_invalid(self):
        self.__test_post_token_invalid('json')

    def test_post_token_yaml_invalid(self):
        self.__test_post_token_invalid('yaml')

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

    def __test_init_basic(self, file_format):
        token_name = self.token_name()
        filename = str(uuid.uuid4())
        flags = f"--cmd '{util.default_cmd()}' --cmd-type shell --health-check-url /status " \
                f"--name {token_name} --{file_format} --file {filename} "
        cp = cli.init(self.waiter_url, init_flags=flags)
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertIn(f'Writing token {file_format.upper()}', cli.stdout(cp))
        try:
            token_definition = util.load_file(file_format, filename)
            self.logger.info(f'Token definition: {cli.dump(file_format, token_definition)}')
            util.post_token(self.waiter_url, token_name, token_definition)
            try:
                token = util.load_token(self.waiter_url, token_name)
                self.assertEqual(token_name, token['name'])
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

    def test_init_basic_json(self):
        self.__test_init_basic('json')

    def test_init_basic_yaml(self):
        self.__test_init_basic('yaml')

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
            token_definition = util.load_file('json', file.name)
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

    @pytest.mark.xfail
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
            cp, services = cli.show_token_services('json', self.waiter_url, token_name=token_name)
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
            self.assertIsNotNone(re.search('^# Instances\\s+([12])$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search('^Total Memory\\s+(128|384) MiB$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search('^Total CPUs\\s+0\\.([13])$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_1}.+Running.+Not Current$', cli.stdout(cp), re.MULTILINE))
            self.assertIsNotNone(re.search(f'^{service_id_2}.+(Failing|Starting).+Current$',
                                           cli.stdout(cp), re.MULTILINE))

            # Run show without --json and with --no-services
            cp = cli.show(self.waiter_url, token_name, show_flags='--no-services')
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertNotIn('# Services', cli.stdout(cp))
            self.assertNotIn('# Failing', cli.stdout(cp))
            self.assertNotIn('# Instances', cli.stdout(cp))
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
            self.assertFalse(token_data['maintenance'])

            # Delete the token
            util.delete_token(self.waiter_url, token_name)

            # Ensure that tokens does not list our token
            cp, tokens = cli.tokens_data(self.waiter_url)
            # The CLI returns 0 if there are any tokens 
            # owned by the user and 1 if there are none
            self.assertIn(cp.returncode, [0, 1], cp.stderr)
            self.assertFalse(any(t['token'] == token_name for t in tokens))
        finally:
            util.delete_token(self.waiter_url, token_name, assert_response=False)

    def __test_tokens_maintenance(self, expected_maintenance_value, service_config={}):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, util.minimal_service_description(**service_config))
        try:
            cp = cli.tokens(self.waiter_url)
            stdout = cli.stdout(cp)
            lines = stdout.split('\n')
            title_line = lines[0]
            maintenance_index = title_line.index('Maintenance')
            line_with_token = next(line for line in lines if token_name in line)
            token_maintenance = line_with_token[maintenance_index:maintenance_index + len(expected_maintenance_value)]
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertEqual(token_maintenance, expected_maintenance_value)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_tokens_token_in_maintenance(self):
        service_config = {"maintenance": {"message": "custom message"}}
        self.__test_tokens_maintenance("True", service_config=service_config)

    def test_tokens_token_not_in_maintenance(self):
        self.__test_tokens_maintenance("False")

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

    def __test_create_token_containing_token_name(self, file_format):
        token_name = self.token_name()
        with cli.temp_token_file({'token': token_name, 'cpus': 0.1, 'mem': 128}, file_format) as path:
            cp = cli.create(self.waiter_url, create_flags=f'--{file_format} {path}')
            self.assertEqual(0, cp.returncode, cp.stderr)
            try:
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.1, token_data['cpus'])
                self.assertEqual(128, token_data['mem'])
            finally:
                util.delete_token(self.waiter_url, token_name)

    def test_create_token_json_containing_token_name(self):
        self.__test_create_token_containing_token_name('json')

    def test_create_token_yaml_containing_token_name(self):
        self.__test_create_token_containing_token_name('yaml')

    def __test_update_token_containing_token_name(self, file_format):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'})
        try:
            with cli.temp_token_file({'token': token_name, 'cpus': 0.2, 'mem': 256}, file_format) as path:
                cp = cli.update(self.waiter_url, update_flags=f'--{file_format} {path}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.2, token_data['cpus'])
                self.assertEqual(256, token_data['mem'])
                self.assertEqual('foo', token_data['cmd'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_update_token_json_containing_token_name(self):
        self.__test_update_token_containing_token_name('json')

    def test_update_token_yaml_containing_token_name(self):
        self.__test_update_token_containing_token_name('yaml')

    def __test_update_token_override_fail(self, file_format):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'})
        try:
            with cli.temp_token_file({'token': token_name, 'cpus': 0.2, 'mem': 256}, file_format) as path:
                cp = cli.update(self.waiter_url, update_flags=f'--cpus 0.3 --{file_format} {path}')
                self.assertEqual(1, cp.returncode, cp.stderr)
                stderr = cli.stderr(cp)
                err_msg = 'You cannot specify the same parameter in both an input file ' \
                          'and token field flags at the same time'
                self.assertIn(err_msg, stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.1, token_data['cpus'])
                self.assertEqual(128, token_data['mem'])
                self.assertEqual('foo', token_data['cmd'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_update_token_json_override_fail(self):
        self.__test_update_token_override_fail('json')

    def test_update_token_yaml_override_fail(self):
        self.__test_update_token_override_fail('yaml')

    def __test_update_token_override_success(self, file_format, diff_token_in_file):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'})
        try:
            token_in_file = f'abc_{token_name}' if diff_token_in_file else token_name
            with cli.temp_token_file({'token': token_in_file, 'cpus': 0.2, 'mem': 256}, file_format) as path:
                update_flags = f'--override --cpus 0.3 --{file_format} {path}'
                if diff_token_in_file:
                    update_flags = f'{update_flags} {token_name}'
                cp = cli.update(self.waiter_url, flags='--verbose', update_flags=update_flags)
                self.assertEqual(0, cp.returncode, cp.stderr)
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0.3, token_data['cpus'])
                self.assertEqual(256, token_data['mem'])
                self.assertEqual('foo', token_data['cmd'])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_update_token_json_parameter_override_success(self):
        self.__test_update_token_override_success('json', False)

    def test_update_token_yaml_parameter_override_success(self):
        self.__test_update_token_override_success('yaml', False)

    def test_update_token_json_token_override_success(self):
        self.__test_update_token_override_success('json', True)

    def test_update_token_yaml_token_override_success(self):
        self.__test_update_token_override_success('yaml', True)

    def __test_update_token_override_failure(self, file_format, diff_token_in_file):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'})
        try:
            token_in_file = f'abc_{token_name}' if diff_token_in_file else token_name
            with cli.temp_token_file({'token': token_in_file, 'cpus': 0.2, 'mem': 256}, file_format) as path:
                update_flags = f'--no-override --cpus 0.3 --{file_format} {path}'
                if diff_token_in_file:
                    update_flags = f'{update_flags} {token_name}'
                cp = cli.update(self.waiter_url, flags='--verbose', update_flags=update_flags)
                self.assertEqual(1, cp.returncode, cp.stderr)
                stderr = cli.stderr(cp)
                err_msg = 'You cannot specify the same parameter'
                self.assertIn(err_msg, stderr)
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_update_token_json_parameter_override_failure(self):
        self.__test_update_token_override_failure('json', False)

    def test_update_token_yaml_parameter_override_failure(self):
        self.__test_update_token_override_failure('yaml', False)

    def test_update_token_json_token_override_failure(self):
        self.__test_update_token_override_failure('json', True)

    def test_update_token_yaml_token_override_failure(self):
        self.__test_update_token_override_failure('yaml', True)

    def test_post_token_over_specified_token_name(self):
        token_name = self.token_name()
        with cli.temp_token_file({'token': token_name}) as path:
            cp = cli.create(self.waiter_url, token_name, create_flags=f'--json {path}')
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('cannot specify the token name both as an argument and in the input file',
                          cli.stderr(cp))

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
        iso_8601_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        custom_fields = {
            'owner': getpass.getuser(),
            'cluster': 'test_show_service_current',
            'root': 'test_show_service_current',
            'last-update-user': getpass.getuser(),
            'last-update-time': iso_8601_time
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

    def __test_create_update_token_admin_mode(self, action, token_name, admin_mode):
        token_fields = {
            'cpus': 0.2,
            'mem': 256,
            'run-as-user': 'FAKE_USERNAME'
        }
        file_format = 'yaml'
        stdin = cli.dump(file_format, token_fields)
        flags = f'{"--admin " if admin_mode else ""}--{file_format} -'
        temp_env = os.environ.copy()
        temp_env["WAITER_ADMIN"] = 'true'
        cp = getattr(cli, action)(self.waiter_url, token_name, flags='-v', stdin=stdin, env=temp_env,
                                  **{f'{action}_flags': flags})
        if admin_mode:
            try:
                token_data = util.load_token(self.waiter_url, token_name)
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertIn('update-mode=admin', cli.stderr(cp))
                self.assertIn(f'Attempting to {action} token in ADMIN mode', cli.stdout(cp))
                for key, value in token_fields.items():
                    self.assertEqual(value, token_data[key])
            finally:
                util.delete_token(self.waiter_url, token_name)
        else:
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Cannot run as user.', cli.decode(cp.stderr))

    def test_create_token_admin_mode(self):
        self.__test_create_update_token_admin_mode('create', self.token_name(), True)

    def test_create_token_no_admin_mode(self):
        self.__test_create_update_token_admin_mode('create', self.token_name(), False)

    def test_update_token_admin_mode(self):
        token_name = self.token_name()
        create_fields = {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'}
        util.post_token(self.waiter_url, token_name, create_fields)
        self.__test_create_update_token_admin_mode('update', token_name, True)

    def test_update_token_no_admin_mode(self):
        self.__test_create_update_token_admin_mode('update', self.token_name(), False)

    def run_maintenance_start_test(self, start_args='', ping_token=False):
        token_name = self.token_name()
        token_fields = util.minimal_service_description()
        custom_maintenance_message = "custom maintenance message"
        util.post_token(self.waiter_url, token_name, token_fields)
        try:
            if ping_token:
                cp = cli.ping(self.waiter_url, token_name)
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertIn('Pinging token', cli.stdout(cp))
                self.assertIn('successful', cli.stdout(cp))
                util.wait_until_services_for_token(self.waiter_url, token_name, 1)
                self.assertEqual(1, len(util.services_for_token(self.waiter_url, token_name)))
            cp = cli.maintenance('start', token_name, self.waiter_url,
                                 maintenance_flags=f'{start_args} "{custom_maintenance_message}"')
            self.assertEqual(0, cp.returncode, cp.stderr)
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual({'message': custom_maintenance_message}, token_data['maintenance'])
            for key, value in token_fields.items():
                self.assertEqual(value, token_data[key])
            if ping_token:
                num_services = 1 if '--no-kill' in start_args else 0
                self.assertEqual(num_services,
                                 len(util.wait_until_services_for_token(self.waiter_url, token_name, num_services)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_maintenance_start_basic(self):
        self.run_maintenance_start_test()

    def test_maintenance_start_no_service_ask_kill(self):
        self.run_maintenance_start_test(start_args='--ask-kill')

    def test_maintenance_start_no_service_force_kill(self):
        self.run_maintenance_start_test(start_args='--force-kill')

    def test_maintenance_start_no_service_no_kill(self):
        self.run_maintenance_start_test(start_args='--no-kill')

    def test_maintenance_start_ping_service_ask_kill(self):
        self.run_maintenance_start_test(start_args='--ask-kill', ping_token=True)

    def test_maintenance_start_ping_service_force_kill(self):
        self.run_maintenance_start_test(start_args='--force-kill', ping_token=True)

    def test_maintenance_start_ping_service_no_kill(self):
        self.run_maintenance_start_test(start_args='--no-kill', ping_token=True)

    def test_maintenance_start_nonexistent_token(self):
        token_name = self.token_name()
        custom_maintenance_message = "custom maintenance message"
        cp = cli.maintenance('start', token_name, self.waiter_url,
                             maintenance_flags=f'"{custom_maintenance_message}"')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('The token does not exist. You must create it first.', cli.stderr(cp))

    def test_maintenance_start_no_cluster(self):
        custom_maintenance_message = "custom maintenance message"
        self.__test_no_cluster(partial(cli.maintenance, 'start',
                                       maintenance_flags=f'"{custom_maintenance_message}"'))

    def test_maintenance_stop_no_ping(self):
        token_name = self.token_name()
        token_fields = {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'}
        custom_maintenance_message = "custom maintenance message"
        util.post_token(self.waiter_url, token_name,
                        {**token_fields, 'maintenance': {'message': custom_maintenance_message}})
        try:
            cp = cli.maintenance('stop', token_name, self.waiter_url, maintenance_flags='--no-ping')
            self.assertEqual(0, cp.returncode, cp.stderr)
            stdout = cli.stdout(cp)
            self.assertNotIn(f'Pinging token {token_name}', stdout)
            self.assertNotIn(f'Ping successful', stdout)
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual(None, token_data.get('maintenance', None))
            for key, value in token_fields.items():
                self.assertEqual(value, token_data[key])
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_maintenance_stop_with_ping(self):
        token_name = self.token_name()
        token_fields = util.minimal_service_description()
        custom_maintenance_message = "custom maintenance message"
        util.post_token(self.waiter_url, token_name,
                        {**token_fields, 'maintenance': {'message': custom_maintenance_message}})
        try:
            cp = cli.maintenance('stop', token_name, self.waiter_url)
            stdout = cli.stdout(cp)
            self.assertEqual(0, cp.returncode, cp.stderr)
            self.assertIn(f'Pinging token {token_name}', stdout)
            self.assertIn('Ping successful', stdout)
            token_data = util.load_token(self.waiter_url, token_name)
            self.assertEqual(None, token_data.get('maintenance', None))
            for key, value in token_fields.items():
                self.assertEqual(value, token_data[key])
            self.assertEqual(1, len(util.wait_until_services_for_token(self.waiter_url, token_name, 1)))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_maintenance_stop_no_cluster(self):
        self.__test_no_cluster(partial(cli.maintenance, 'stop'))

    def test_maintenance_stop_not_in_maintenance(self):
        token_name = self.token_name()
        util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'})
        try:
            cp = cli.maintenance('stop', token_name, self.waiter_url)
            self.assertEqual(1, cp.returncode, cp.stderr)
            self.assertIn('Token is not in maintenance mode', cli.stderr(cp))
        finally:
            util.delete_token(self.waiter_url, token_name)

    def __test_maintenance_check(self, maintenance_active):
        token_name = self.token_name()
        output = f'{token_name} is {"" if maintenance_active else "not "}in maintenance mode'
        cli_return_code = 0 if maintenance_active else 1
        if maintenance_active:
            util.post_token(self.waiter_url, token_name,
                            {'cpus': 0.1, 'mem': 128, 'cmd': 'foo', 'maintenance': {'message': 'custom message'}})
        else:
            util.post_token(self.waiter_url, token_name, {'cpus': 0.1, 'mem': 128, 'cmd': 'foo'})
        try:
            cp = cli.maintenance('check', token_name, self.waiter_url)
            self.assertEqual(cli_return_code, cp.returncode, cp.stderr)
            self.assertIn(output, cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name)

    def test_maintenance_check_not_in_maintenance_mode(self):
        self.__test_maintenance_check(False)

    def test_maintenance_check_in_maintenance_mode(self):
        self.__test_maintenance_check(True)

    def test_maintenance_no_sub_command(self):
        cp = cli.maintenance('', '')
        cp_help = cli.maintenance('', '', maintenance_flags='-h')
        self.assertEqual(0, cp.returncode, cp.stderr)
        self.assertEqual(cli.stdout(cp_help), cli.stdout(cp))

    def __test_ssh_instance_id(self, instance_fn, no_data=False, command_to_run=None, is_failed_instance=False):
        token_name = self.token_name()
        token_fields = util.minimal_service_description()
        if is_failed_instance:
            token_fields['cmd'] = 'this_is_an_invalid_command'
        util.post_token(self.waiter_url, token_name, token_fields)
        try:
            service_id = util.ping_token(self.waiter_url, token_name,
                                         expected_status_code=503 if is_failed_instance else 200)
            instances = util.instances_for_service(self.waiter_url, service_id)
            if is_failed_instance:
                self.assertEqual(0, len(instances['active-instances']))
                self.assertLess(0, len(instances['failed-instances']))
            else:
                self.assertEqual(1, len(instances['active-instances']))
                self.assertEqual(0, len(instances['failed-instances']))
            self.assertEqual(0, len(instances['killed-instances']))

            # ssh into instance
            env = os.environ.copy()
            env["WAITER_SSH"] = 'echo'
            env["WAITER_KUBECTL"] = 'echo'
            instance = instance_fn(service_id, instances)
            cp = cli.ssh(self.waiter_url, instance['id'], ssh_command=command_to_run, ssh_flags='-i', env=env)
            if no_data:
                self.assertEqual(1, cp.returncode, cp.stderr)
                self.assertIn('No matching data found', cli.stdout(cp))
            else:
                log_directory = instance['log-directory']
                self.assertEqual(0, cp.returncode, cp.stderr)
                if util.using_kubernetes(self.waiter_url):
                    api_server = instance['k8s/api-server-url']
                    namespace = instance['k8s/namespace']
                    pod_name = instance['k8s/pod-name']
                    self.assertIn(f'--server {api_server} --namespace {namespace} exec -it {pod_name} -c -- '
                                  f"/bin/bash -c cd {log_directory}; {command_to_run or 'exec /bin/bash'}",
                                  cli.stdout(cp))
                else:
                    self.assertIn(f"-t {instance['host']} cd {log_directory} ; {command_to_run or '/bin/bash'}",
                                  cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_ssh_instance_id(self):
        self.__test_ssh_instance_id(lambda _, instances: instances['active-instances'][0])

    def test_ssh_instance_id_failed_instance(self):
        self.__test_ssh_instance_id(lambda _, instances: instances['failed-instances'][0], is_failed_instance=True)

    def test_ssh_instance_id_custom_cmd(self):
        self.__test_ssh_instance_id(lambda _, instances: instances['active-instances'][0],
                                    command_to_run='ls -al')

    def test_ssh_instance_id_custom_cmd_failed_instance(self):
        self.__test_ssh_instance_id(lambda _, instances: instances['failed-instances'][0], is_failed_instance=True,
                                    command_to_run='ls -al')

    def test_ssh_instance_id_no_instance(self):
        self.__test_ssh_instance_id(lambda service_id, _: {'id': service_id + '.nonexistent'}, no_data=True)

    def test_ssh_instance_id_no_service(self):
        instance_id_no_service = "a.a"
        cp = cli.ssh(self.waiter_url, instance_id_no_service, ssh_flags='-i')
        self.assertEqual(1, cp.returncode, cp.stderr)
        self.assertIn('No matching data found', cli.stdout(cp))

    def __test_ssh_service_id(self, command_to_run=None, min_instances=1, stdin=None):
        token_name = self.token_name()
        token_fields = util.minimal_service_description()
        if min_instances:
            token_fields['min-instances'] = min_instances
        util.post_token(self.waiter_url, token_name, token_fields)
        try:
            service_id = util.ping_token(self.waiter_url, token_name)
            util.wait_until_instances_for_service(self.waiter_url, service_id,
                                                  {'active-instances': min_instances,
                                                   'failed-instances': 0,
                                                   'killed-instances': 0})
            instances = util.instances_for_service(self.waiter_url, service_id)

            # ssh into instance
            env = os.environ.copy()
            env["WAITER_SSH"] = 'echo'
            env["WAITER_KUBECTL"] = 'echo'
            instance = instances['active-instances'][0]
            cp = cli.ssh(self.waiter_url, service_id, ssh_command=command_to_run, ssh_flags='-s', stdin=stdin, env=env)
            log_directory = instance['log-directory']
            self.assertEqual(0, cp.returncode, cp.stderr)
            if util.using_kubernetes(self.waiter_url):
                api_server = instance['k8s/api-server-url']
                namespace = instance['k8s/namespace']
                pod_name = instance['k8s/pod-name']
                self.assertIn(f'--server {api_server} --namespace {namespace} exec -it {pod_name} -c -- '
                              f"/bin/bash -c cd {log_directory}; {command_to_run or 'exec /bin/bash'}",
                              cli.stdout(cp))
            else:
                self.assertIn(f"-t {instance['host']} cd {log_directory} ; {command_to_run or '/bin/bash'}",
                              cli.stdout(cp))
        finally:
            util.delete_token(self.waiter_url, token_name, kill_services=True)

    def test_ssh_service_id_single_instance(self):
        self.__test_ssh_service_id()

    def test_ssh_service_id_multiple_instances(self):
        self.__test_ssh_service_id(min_instances=2, stdin='0\n'.encode('utf8'))

    def test_ssh_service_id_no_instances(self):
        self.assertTrue(False)

    def test_ssh_token_single_cluster(self):
        self.asserTrue(False)

    def test_ssh_token_multiple_clusters(self):
        self.asserTrue(False)

    def test_ssh_token_single_service(self):
        self.asserTrue(False)

    def test_ssh_token_multiple_services(self):
        self.asserTrue(False)
