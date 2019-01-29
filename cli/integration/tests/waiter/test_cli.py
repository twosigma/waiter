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

        # Make sure token doesn't exist
        util.delete_token(self.waiter_url, token_name, assert_response=False)
        error = util.load_token(self.waiter_url, token_name, expected_status_code=404)
        self.assertEqual(f"Couldn't find token {token_name}", error['waiter-error']['message'])

        # Create token
        response_text = str(uuid.uuid4())
        cmd = util.minimal_service_cmd(response_text=response_text)
        service = util.minimal_service_description(cmd=cmd)
        cp = cli.create_from_service_description(token_name, self.waiter_url, service)
        self.assertEqual(0, cp.returncode, cp.stderr)
        try:
            # Make sure token now exists
            token = util.load_token(self.waiter_url, token_name)
            self.assertIsNotNone(token)
            self.assertEqual(cmd, token['cmd'])

            # Make sure we can access the service
            resp = util.session.get(self.waiter_url, headers={'X-Waiter-Token': token_name})
            self.assertEqual(200, resp.status_code)
            self.assertEqual(response_text, resp.text)
        finally:
            util.delete_token(self.waiter_url, token_name)
