import json
import logging
import os
import pty
import shlex

# Manually create a TTY that we can use as the default STDIN
import subprocess
import tempfile
from fcntl import fcntl, F_GETFL, F_SETFL

from tests.waiter import util

_STDIN_TTY = pty.openpty()[1]


def decode(b):
    """Decodes as UTF-8"""
    return b.decode('UTF-8')


def stdout(cp):
    """Returns the UTF-8 decoded and stripped stdout of the given CompletedProcess"""
    return decode(cp.stdout).strip()


def stderr(cp):
    """Returns the UTF-8 decoded and stripped stderr of the given CompletedProcess"""
    return decode(cp.stderr).strip()


def sh(cmd, stdin=None, env=None, wait_for_exit=True):
    """Runs command using subprocess.run"""
    logging.info(cmd + (f' # stdin: {decode(stdin)}' if stdin else ''))
    command_args = shlex.split(cmd)
    if wait_for_exit:
        # We manually attach stdin to a TTY if there is no piped input
        # since the default stdin isn't guaranteed to be a TTY.
        input_args = {'input': stdin} if stdin is not None else {'stdin': _STDIN_TTY}
        cp = subprocess.run(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, **input_args)
        return cp
    else:
        proc = subprocess.Popen(command_args, stdin=_STDIN_TTY, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Get the current stdout, stderr flags
        stdout_flags = fcntl(proc.stdout, F_GETFL)
        stderr_flags = fcntl(proc.stderr, F_GETFL)
        # Set the O_NONBLOCK flag of the stdout, stderr file descriptors
        # (if we don't set this, calls to readlines() will block)
        fcntl(proc.stdout, F_SETFL, stdout_flags | os.O_NONBLOCK)
        fcntl(proc.stderr, F_SETFL, stderr_flags | os.O_NONBLOCK)
        return proc


def command():
    """If the WAITER_TEST_CLI_COMMAND environment variable is set, returns its value, otherwise 'waiter'"""
    return os.getenv('WAITER_TEST_CLI_COMMAND', 'waiter')


def cli(args, waiter_url=None, flags=None, stdin=None, env=None, wait_for_exit=True):
    """Runs a CLI command with the given URL, flags, and stdin"""
    url_flag = f'--url {waiter_url}' if waiter_url else ''
    other_flags = f'{flags}' if flags else ''
    cp = sh(f'{command()} {url_flag} {other_flags} {args}', stdin, env, wait_for_exit)
    logging.debug(output(cp))
    return cp


def create_or_update(subcommand, waiter_url=None, token_name=None, flags=None, create_flags=None):
    """Creates or updates a token via the CLI"""
    args = f"{subcommand} {token_name} {create_flags or ''}"
    cp = cli(args, waiter_url, flags)
    return cp


def create(waiter_url=None, token_name=None, flags=None, create_flags=None):
    """Creates a token via the CLI"""
    cp = create_or_update('create', waiter_url, token_name, flags, create_flags)
    return cp


def update(waiter_url=None, token_name=None, flags=None, create_flags=None):
    """Updates a token via the CLI"""
    cp = create_or_update('update', waiter_url, token_name, flags, create_flags)
    return cp


def create_or_update_from_service_description(subcommand, waiter_url, token_name, service, flags=None):
    """Creates or updates a token via the CLI, using the provided service fields"""
    create_flags = \
        (f"--cmd '{service['cmd']}' " if 'cmd' in service else '') + \
        (f"--cpus {service['cpus']} " if 'cpus' in service else '') + \
        (f"--mem {service['mem']} " if 'mem' in service else '') + \
        (f"--cmd-type {service['cmd-type']} " if 'cmd-type' in service else '') + \
        (f"--version {service['version']}" if 'version' in service else '')
    cp = create_or_update(subcommand, waiter_url, token_name, flags=flags, create_flags=create_flags)
    return cp


def create_from_service_description(waiter_url, token_name, service, flags=None):
    """Creates a token via the CLI, using the provided service fields"""
    cp = create_or_update_from_service_description('create', waiter_url, token_name, service, flags)
    return cp


def update_from_service_description(waiter_url, token_name, service, flags=None):
    """Updates a token via the CLI, using the provided service fields"""
    cp = create_or_update_from_service_description('update', waiter_url, token_name, service, flags)
    return cp


def create_or_update_minimal(subcommand, waiter_url=None, token_name=None, flags=None, **kwargs):
    """Creates or updates a token via the CLI, using the "minimal" service description"""
    service = util.minimal_service_description(**kwargs)
    cp = create_or_update_from_service_description(subcommand, waiter_url, token_name, service, flags=flags)
    return cp


def create_minimal(waiter_url=None, token_name=None, flags=None, **kwargs):
    """Creates a token via the CLI, using the "minimal" service description"""
    cp = create_or_update_minimal('create', waiter_url, token_name, flags, **kwargs)
    return cp


def update_minimal(waiter_url=None, token_name=None, flags=None, **kwargs):
    """Updates a token via the CLI, using the "minimal" service description"""
    cp = create_or_update_minimal('update', waiter_url, token_name, flags, **kwargs)
    return cp


def write_json(path, config):
    """Writes the given config map as JSON to the given path."""
    with open(path, 'w') as outfile:
        logging.info('echo \'%s\' > %s' % (json.dumps(config), path))
        json.dump(config, outfile)


class temp_config_file:
    """
    A context manager used to generate and subsequently delete a temporary
    config file for the CLI. Takes as input the config dictionary to use.
    """

    def __init__(self, config):
        self.config = config

    def deep_merge(self, a, b):
        """Merges a and b, letting b win if there is a conflict"""
        merged = a.copy()
        for key in b:
            b_value = b[key]
            merged[key] = b_value
            if key in a:
                a_value = a[key]
                if isinstance(a_value, dict) and isinstance(b_value, dict):
                    merged[key] = self.deep_merge(a_value, b_value)
        return merged

    def write_temp_json(self):
        path = tempfile.NamedTemporaryFile(delete=False).name
        config = self.deep_merge(base_config(), self.config)
        write_json(path, config)
        return path

    def __enter__(self):
        self.path = self.write_temp_json()
        return self.path

    def __exit__(self, _, __, ___):
        os.remove(self.path)


class temp_token_file:
    """
    A context manager used to generate and subsequently delete a temporary
    token JSON file for the CLI. Takes as input the token dictionary to use.
    """

    def __init__(self, token_data):
        self.token_data = token_data

    def write_temp_json(self):
        path = tempfile.NamedTemporaryFile(delete=False).name
        write_json(path, self.token_data)
        return path

    def __enter__(self):
        self.path = self.write_temp_json()
        return self.path

    def __exit__(self, _, __, ___):
        os.remove(self.path)


def show(waiter_url=None, token_name=None, flags=None, show_flags=None):
    """Shows a token via the CLI"""
    args = f"show {token_name} {show_flags or ''}"
    cp = cli(args, waiter_url, flags)
    return cp


def __show_json(waiter_url=None, token_name=None, flags=None):
    """Invokes show on the given token with --json, and returns the parsed JSON"""
    flags = (flags + ' ') if flags else ''
    cp = show(waiter_url, token_name, flags, '--json')
    data = json.loads(stdout(cp))
    return cp, data


def show_token(waiter_url=None, token_name=None, flags=None):
    """Shows the token JSON corresponding to the given token name"""
    cp, data = __show_json(waiter_url, token_name, flags)
    tokens = [entities['token'] for entities in data['clusters'].values()]
    return cp, tokens


def output(cp):
    """Returns a string containing the stdout and stderr from the given CompletedProcess"""
    return f'\nstdout:\n{stdout(cp)}\n\nstderr:\n{decode(cp.stderr)}'


def plugins_config():
    """
    If the WAITER_TEST_PLUGIN_JSON environment variable is set,
    returns the parsed contents of the file, otherwise empty dict
    """
    if 'WAITER_TEST_PLUGIN_JSON' in os.environ:
        path = os.environ['WAITER_TEST_PLUGIN_JSON']
        content = util.load_json_file(os.path.abspath(path))
        return content or {}
    else:
        return {}


def base_config():
    """Returns a "base" config map that can be added to."""
    return plugins_config()


def write_base_config():
    """Creates a config file that can be used as a starting point for integration tests"""
    config = base_config()
    if config:
        write_json(os.path.abspath('.waiter.json'), config)


class temp_base_config_file:
    """
    A context manager used to generate and subsequently delete a temporary
    base config file for the CLI. Takes as input the config dictionary to use.
    """

    def __init__(self, config):
        # Get the location of the waiter executable so we can add a default `.waiter.json` file
        cp = subprocess.run(args=['which', command()], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        base_exec = cp.stdout.decode("utf-8").rstrip('\n')
        base_dir = os.path.dirname(os.path.abspath(base_exec))
        self.path = os.path.join(base_dir, '.waiter.json')
        self.config = config

    def __enter__(self):
        write_json(self.path, self.config)
        return self.path

    def __exit__(self, _, __, ___):
        os.remove(self.path)


def delete(waiter_url=None, token_name=None, flags=None, delete_flags=None):
    """Deletes a token via the CLI"""
    args = f"delete {token_name} {delete_flags or ''}"
    cp = cli(args, waiter_url, flags)
    return cp


def ping(waiter_url=None, token_name_or_service_id=None, flags=None, ping_flags=None):
    """Pings a token via the CLI"""
    args = f'ping {token_name_or_service_id or ""} {ping_flags or ""}'
    cp = cli(args, waiter_url, flags)
    return cp


def kill(waiter_url=None, token_name_or_service_id=None, flags=None, kill_flags=None):
    """Kills services using a token via the CLI"""
    args = f'kill {token_name_or_service_id} {kill_flags or ""}'
    cp = cli(args, waiter_url, flags)
    return cp


def init(waiter_url=None, flags=None, init_flags=None):
    """Creates a barebones token JSON file via the CLI"""
    args = f'init {init_flags or ""}'
    cp = cli(args, waiter_url, flags)
    return cp
