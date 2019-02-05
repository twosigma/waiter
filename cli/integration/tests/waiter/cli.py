import logging
import os
import pty
import shlex

# Manually create a TTY that we can use as the default STDIN
import subprocess
from fcntl import fcntl, F_GETFL, F_SETFL

_STDIN_TTY = pty.openpty()[1]


def decode(b):
    """Decodes as UTF-8"""
    return b.decode('UTF-8')


def stdout(cp):
    """Returns the UTF-8 decoded and stripped stdout of the given CompletedProcess"""
    return decode(cp.stdout).strip()


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
    return os.environ['WAITER_TEST_CLI_COMMAND'] if 'WAITER_TEST_CLI_COMMAND' in os.environ else 'waiter'


def cli(args, waiter_url=None, flags=None, stdin=None, env=None, wait_for_exit=True):
    """Runs a CLI command with the given URL, flags, and stdin"""
    url_flag = f'--url {waiter_url} ' if waiter_url else ''
    other_flags = f'{flags} ' if flags else ''
    cp = sh(f'{command()} {url_flag}{other_flags}{args}', stdin, env, wait_for_exit)
    return cp


def create(token_name, waiter_url=None, flags=None, create_flags=None):
    """Creates a token via the CLI"""
    args = f"create {token_name} {create_flags or ''}"
    cp = cli(args, waiter_url, flags)
    return cp


def create_from_service_description(token_name, waiter_url, service):
    """Creates a token via the CLI, using the provided service fields"""
    cp = create(token_name, waiter_url, create_flags=f"--cmd '{service['cmd']}' "
                                                     f"--cpus {service['cpus']} "
                                                     f"--mem {service['mem']} "
                                                     f"--cmd-type {service['cmd-type']} "
                                                     f"--version {service['version']}")
    return cp
