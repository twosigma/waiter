import logging
import os

import pytest

logging.info('Checking if test-metric recording needs to be enabled')
if 'TEST_METRICS_URL' in os.environ:
    import datetime
    import getpass
    import json
    import socket
    from timeit import default_timer as timer

    from tests.waiter import util

    elastic_search_url = os.getenv('TEST_METRICS_URL').rstrip('/')
    logging.info(f'Sending test metrics to {elastic_search_url}')
    #print("xxx initializng failed_tests")
    #failed_tests = []
    #print("xxx done initializng failed_tests")


    def pytest_configure():
        pytest.my_failed_tests = []
    
    @pytest.fixture(scope='session', autouse=True)
    def session_hook():
        # Executed before tests
        yield
        # Executed after tests
        failed_tests_file = os.getenv('TEST_METRICS_FAILED_TESTS_FILE', None)
        if failed_tests_file is None:
            try:
                os.makedirs('.test_metrics')
            except:
                pass
            failed_tests_file = '.test_metrics/last_failed_tests'
        with open(failed_tests_file, 'w') as file_handle:
            json.dump({'failed-tests': pytest.my_failed_tests}, file_handle)

    @pytest.fixture()
    def record_test_metric(request):
        start = timer()
        yield
        try:
            end = timer()
            now = datetime.datetime.utcnow()
            index = f'waiter-tests-{now.strftime("%Y%m%d")}'
            request_node = request.node
            xfail_mark = request_node._evalxfail._mark
            test_namespace = '.'.join(request_node._nodeid.split('::')[:-1]).replace('/', '.').replace('.py', '')
            test_name = request_node.name
            setup = request_node.rep_setup
            call = request_node.rep_call
            if setup.failed or call.failed:
                result = 'failed'
            elif setup.passed and call.passed:
                result = 'passed'
            elif call.skipped:
                result = 'skipped'
            else:
                logging.warning('Unable to determine test result')
                result = 'unknown'
            if result == 'failed':
                pytest.my_failed_tests.append(request_node._nodeid)
            metrics = {
                'build-id': os.getenv('TEST_METRICS_BUILD_ID', None),
                'expected-to-fail': xfail_mark is not None and xfail_mark.name == 'xfail',
                'git-branch': os.getenv('TEST_METRICS_BRANCH', None),
                'git-branch-under-test': os.getenv('TEST_METRICS_BRANCH_UNDER_TEST', None),
                'git-commit-hash': os.getenv('TEST_METRICS_COMMIT_HASH', None),
                'git-commit-hash-under-test': os.getenv('TEST_METRICS_COMMIT_HASH_UNDER_TEST', None),
                'host': socket.gethostname(),
                'project': 'waiter-cli',
                'result': result,
                'run-attempt': os.getenv('TEST_METRICS_RUN_ATTEMPT', None),
                'run-description': os.getenv('TEST_METRICS_RUN_DESCRIPTION', 'open source CLI integration tests'),
                'run-id': os.getenv('TEST_METRICS_RUN_ID', None),
                'runtime-milliseconds': (end - start)*1000,
                'test-name': test_name,
                'test-namespace': test_namespace,
                'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S'),
                'user': getpass.getuser()
            }
            logging.info(f'Updating test metrics: {json.dumps(metrics, indent=2)}')
            resp = util.session.post(f'{elastic_search_url}/{index}/test-result', json=metrics)
            logging.info(f'Response from updating test metrics: {resp.text}')
        except:
            logging.exception('Encountered exception while recording test metrics')


    @pytest.hookimpl(tryfirst=True, hookwrapper=True)
    def pytest_runtest_makereport(item, call):
        # execute all other hooks to obtain the report object
        outcome = yield
        rep = outcome.get_result()

        # set a report attribute for each phase of a call, which can
        # be "setup", "call", "teardown"
        setattr(item, "rep_" + rep.when, rep)
else:
    logging.info('Test-metric recording is not getting enabled')


    @pytest.fixture()
    def record_test_metric():
        pass
