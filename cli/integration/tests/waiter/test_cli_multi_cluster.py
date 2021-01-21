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
                cp, tokens = cli.show_token('json', token_name=token_name, flags='--config %s' % path)
                versions = [t['version'] for t in tokens]
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertEqual(1, len(tokens), tokens)
                self.assertIn(version_1, versions)

                # Create in cluster #2
                version_2 = str(uuid.uuid4())
                util.post_token(self.waiter_url_2, token_name, {'version': version_2})
                try:
                    # Again, single query for the token name, federated across clusters
                    cp, tokens = cli.show_token('json', token_name=token_name, flags='--config %s' % path)
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

    def test_delete_single_cluster(self):
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
                    # Delete the token in one cluster only
                    cp = cli.delete(token_name=token_name, flags=f'--config {path} --cluster waiter2')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertNotIn('exists in 2 clusters', cli.stdout(cp))
                    self.assertNotIn('waiter1', cli.stdout(cp))
                    self.assertIn('waiter2', cli.stdout(cp))
                    self.assertEqual(1, cli.stdout(cp).count('Deleting token'))
                    self.assertEqual(1, cli.stdout(cp).count('Successfully deleted'))
                    util.load_token(self.waiter_url_1, token_name, expected_status_code=200)
                    util.load_token(self.waiter_url_2, token_name, expected_status_code=404)
            finally:
                util.delete_token(self.waiter_url_2, token_name, assert_response=False)
        finally:
            util.delete_token(self.waiter_url_1, token_name, assert_response=True)

    def test_federated_ping(self):
        # Create in cluster #1
        token_name = self.token_name()
        util.post_token(self.waiter_url_1, token_name, util.minimal_service_description())
        try:
            # Create in cluster #2
            util.post_token(self.waiter_url_2, token_name, util.minimal_service_description())
            try:
                config = self.__two_cluster_config()
                with cli.temp_config_file(config) as path:
                    # Ping the token in both clusters
                    cp = cli.ping(token_name_or_service_id=token_name, flags=f'--config {path}')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn('waiter1', cli.stdout(cp))
                    self.assertIn('waiter2', cli.stdout(cp))
                    self.assertEqual(2, cli.stdout(cp).count('Pinging token'))
                    self.assertEqual(1, len(util.services_for_token(self.waiter_url_1, token_name)))
                    self.assertEqual(1, len(util.services_for_token(self.waiter_url_2, token_name)))
            finally:
                util.delete_token(self.waiter_url_2, token_name, kill_services=True)
        finally:
            util.delete_token(self.waiter_url_1, token_name, kill_services=True)

    def test_federated_kill(self):
        # Create in cluster #1
        token_name = self.token_name()
        util.post_token(self.waiter_url_1, token_name, util.minimal_service_description())
        try:
            # Create in cluster #2
            util.post_token(self.waiter_url_2, token_name, util.minimal_service_description())
            try:
                # Ping the token in both clusters
                util.ping_token(self.waiter_url_1, token_name)
                util.ping_token(self.waiter_url_2, token_name)

                # Kill the services in both clusters
                config = self.__two_cluster_config()
                with cli.temp_config_file(config) as path:
                    cp = cli.kill(token_name_or_service_id=token_name, flags=f'--config {path}', kill_flags='--force')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn('waiter1', cli.stdout(cp))
                    self.assertIn('waiter2', cli.stdout(cp))
                    self.assertEqual(2, cli.stdout(cp).count('Killing service'))
                    self.assertEqual(2, cli.stdout(cp).count('Successfully killed'))
                    self.assertEqual(0, len(util.services_for_token(self.waiter_url_1, token_name)))
                    self.assertEqual(0, len(util.services_for_token(self.waiter_url_2, token_name)))
            finally:
                util.delete_token(self.waiter_url_2, token_name, kill_services=True)
        finally:
            util.delete_token(self.waiter_url_1, token_name, kill_services=True)

    def test_federated_kill_service_id(self):
        # Create in cluster #1
        token_name = self.token_name()
        service_description = util.minimal_service_description()
        util.post_token(self.waiter_url_1, token_name, service_description)
        try:
            # Create in cluster #2
            util.post_token(self.waiter_url_2, token_name, service_description)
            try:
                # Ping the token in both clusters
                service_id_1 = util.ping_token(self.waiter_url_1, token_name)
                service_id_2 = util.ping_token(self.waiter_url_2, token_name)
                self.assertEqual(service_id_1, service_id_2)

                # Kill the services in both clusters
                util.kill_services_using_token(self.waiter_url_1, token_name)
                util.kill_services_using_token(self.waiter_url_2, token_name)

                # Attempt to kill using the CLI
                config = self.__two_cluster_config()
                with cli.temp_config_file(config) as path:
                    # First with --force
                    cp = cli.kill(token_name_or_service_id=service_id_1, flags=f'--config {path}',
                                  kill_flags='--force --service-id')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn('waiter1', cli.stdout(cp))
                    self.assertIn('waiter2', cli.stdout(cp))
                    self.assertEqual(2, cli.stdout(cp).count('cannot be killed because it is already Inactive'))

                    # Then, without --force
                    cp = cli.kill(token_name_or_service_id=service_id_1, flags=f'--config {path}',
                                  kill_flags='--service-id')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn(f'waiter1 / {service_id_1}', cli.stdout(cp))
                    self.assertIn(f'waiter2 / {service_id_2}', cli.stdout(cp))
                    self.assertIn(f'{self.waiter_url_1}/apps/{service_id_1}', cli.stdout(cp))
                    self.assertIn(f'{self.waiter_url_2}/apps/{service_id_2}', cli.stdout(cp))
                    self.assertEqual(2, cli.stdout(cp).count('cannot be killed because it is already Inactive'))
                    self.assertEqual(2, cli.stdout(cp).count('Run as user'))
            finally:
                util.delete_token(self.waiter_url_2, token_name, kill_services=True)
        finally:
            util.delete_token(self.waiter_url_1, token_name, kill_services=True)

    def test_update_non_default_cluster(self):
        # Set up the config so that cluster #1 is the default
        config = {'clusters': [{'name': 'waiter1', 'url': self.waiter_url_1, 'default-for-create': True},
                               {'name': 'waiter2', 'url': self.waiter_url_2}]}

        # Create in cluster #2 (the non-default)
        token_name = self.token_name()
        service_description = util.minimal_service_description()
        util.post_token(self.waiter_url_2, token_name, service_description)
        try:
            # Update using the CLI, which should update in cluster #2
            with cli.temp_config_file(config) as path:
                version = str(uuid.uuid4())
                cp = cli.update(token_name=token_name, flags=f'--config {path}', update_flags=f'--version {version}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertNotIn('waiter1', cli.stdout(cp))
                self.assertIn('waiter2', cli.stdout(cp))
                token_1 = util.load_token(self.waiter_url_1, token_name, expected_status_code=404)
                token_2 = util.load_token(self.waiter_url_2, token_name, expected_status_code=200)
                self.assertNotIn('version', token_1)
                self.assertEqual(version, token_2['version'])
        finally:
            util.delete_token(self.waiter_url_2, token_name)

    def test_update_token_chooses_latest_configured_cluster(self):
        config = {'clusters': [{'name': 'waiter1',
                                'url': self.waiter_url_1,
                                'default-for-create': True,
                                'sync-group': 'staging clusters'},
                               {'name': 'waiter2', 'url': self.waiter_url_2,
                                'sync-group': 'staging clusters'}]}

        cluster_1_name = util.retrieve_waiter_settings(self.waiter_url_1)['cluster-config']['name']
        cluster_2_name = util.retrieve_waiter_settings(self.waiter_url_2)['cluster-config']['name']

        # Create in cluster #1, then cluster #2
        token_name = self.token_name()
        util.post_token(self.waiter_url_1, token_name, util.minimal_service_description(cluster=cluster_1_name))
        util.post_token(self.waiter_url_2, token_name, util.minimal_service_description(cluster=cluster_2_name))
        try:
            # Update using the CLI, which should update in cluster #2
            with cli.temp_config_file(config) as path:
                version = str(uuid.uuid4())
                cp = cli.update(token_name=token_name, flags=f'--config {path}', update_flags=f'--version {version}')
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertNotIn('waiter1', cli.stdout(cp))
                self.assertIn('waiter2', cli.stdout(cp))
                token_1 = util.load_token(self.waiter_url_1, token_name, expected_status_code=200)
                token_2 = util.load_token(self.waiter_url_2, token_name, expected_status_code=200)
                self.assertNotEqual(version, token_1['version'])
                self.assertEqual(version, token_2['version'])
        finally:
            util.delete_token(self.waiter_url_1, token_name)
            util.delete_token(self.waiter_url_2, token_name)


    def test_update_token_multiple_sync_groups(self):
        sync_group_1 = "sync-group-1"
        sync_group_2 = "sync-group-2"
        config = {'clusters': [{'name': 'waiter1',
                                'url': self.waiter_url_1,
                                'default-for-create': True,
                                'sync-group': sync_group_1},
                               {'name': 'waiter2', 'url': self.waiter_url_2,
                                'sync-group': sync_group_2}]}

        cluster_1_name = util.retrieve_waiter_settings(self.waiter_url_1)['cluster-config']['name']
        cluster_2_name = util.retrieve_waiter_settings(self.waiter_url_2)['cluster-config']['name']

        # Create in cluster #1, then cluster #2
        token_name = self.token_name()
        util.post_token(self.waiter_url_1, token_name, util.minimal_service_description(cluster=cluster_1_name))
        util.post_token(self.waiter_url_2, token_name, util.minimal_service_description(cluster=cluster_2_name))
        try:
            # Update using the CLI, which should update in cluster #2
            with cli.temp_config_file(config) as path:
                version = str(uuid.uuid4())
                cp = cli.update(token_name=token_name, flags=f'--config {path}', update_flags=f'--version {version}')
                self.assertEqual(1, cp.returncode, cp.stderr)
                self.assertIn('Could not infer the target cluster', cli.stderr(cp))
                self.assertIn(sync_group_1, cli.stderr(cp))
                self.assertIn(sync_group_2, cli.stderr(cp))
        finally:
            util.delete_token(self.waiter_url_1, token_name)
            util.delete_token(self.waiter_url_2, token_name)


    def test_ping_via_token_cluster(self):
        # Create in cluster #1
        token_name = self.token_name()
        token_data = util.minimal_service_description()
        util.post_token(self.waiter_url_1, token_name, token_data)
        try:
            # Create in cluster #2
            token_data['cluster'] = util.load_token(self.waiter_url_1, token_name)['cluster']
            util.post_token(self.waiter_url_2, token_name, token_data)
            try:
                config = self.__two_cluster_config()
                with cli.temp_config_file(config) as path:
                    # Ping the token, which should only ping in cluster #1
                    cp = cli.ping(token_name_or_service_id=token_name, flags=f'--config {path}')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn('waiter1', cli.stdout(cp))
                    self.assertEqual(1, cli.stdout(cp).count('Pinging token'))
                    self.assertEqual(1, cli.stdout(cp).count('Not pinging token'))
                    self.assertEqual(1, len(util.services_for_token(self.waiter_url_1, token_name)))
                    self.assertEqual(0, len(util.services_for_token(self.waiter_url_2, token_name)))

                    # Ping the token in cluster #2 explicitly
                    cp = cli.ping(token_name_or_service_id=token_name, flags=f'--config {path} --cluster waiter2')
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn('waiter2', cli.stdout(cp))
                    self.assertEqual(1, cli.stdout(cp).count('Pinging token'))
                    self.assertEqual(0, cli.stdout(cp).count('Not pinging token'))
                    self.assertEqual(1, len(util.services_for_token(self.waiter_url_2, token_name)))
            finally:
                util.delete_token(self.waiter_url_2, token_name, kill_services=True)
        finally:
            util.delete_token(self.waiter_url_1, token_name, kill_services=True)

    def test_federated_tokens(self):
        # Create in cluster #1
        token_name = self.token_name()
        util.post_token(self.waiter_url_1, token_name, util.minimal_service_description())
        try:
            # Single query for the tokens, federated across clusters
            cluster_1 = f'foo_{uuid.uuid4()}'
            cluster_2 = f'bar_{uuid.uuid4()}'
            config = {'clusters': [{'name': cluster_1, 'url': self.waiter_url_1},
                                   {'name': cluster_2, 'url': self.waiter_url_2}]}
            with cli.temp_config_file(config) as path:
                cp, tokens = cli.tokens_data(flags='--config %s' % path)
                tokens = [t for t in tokens if t['token'] == token_name]
                self.assertEqual(0, cp.returncode, cp.stderr)
                self.assertEqual(1, len(tokens), tokens)

                # Create in cluster #2
                util.post_token(self.waiter_url_2, token_name, util.minimal_service_description())
                try:
                    # Again, single query for the tokens, federated across clusters
                    cp, tokens = cli.tokens_data(flags='--config %s' % path)
                    tokens = [t for t in tokens if t['token'] == token_name]
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertEqual(2, len(tokens), tokens)

                    # Test the secondary sort on cluster
                    cp = cli.tokens(flags='--config %s' % path)
                    stdout = cli.stdout(cp)
                    self.assertEqual(0, cp.returncode, cp.stderr)
                    self.assertIn(cluster_1, stdout)
                    self.assertIn(cluster_2, stdout)
                    self.assertLess(stdout.index(cluster_2), stdout.index(cluster_1))
                finally:
                    util.delete_token(self.waiter_url_2, token_name)
        finally:
            util.delete_token(self.waiter_url_1, token_name)

