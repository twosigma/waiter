#!/usr/bin/env python3
"""Module implementing a CLI for the Waiter API. """

import logging
import signal
import sys

from waiter.cli import run
from waiter.util import print_error


def main(args=None, plugins={}):
    if args is None:
        args = sys.argv[1:]

    try:
        result = run(args, plugins)
        sys.exit(result)
    except Exception as e:
        logging.exception('exception when running with %s' % args)
        print_error(str(e))
        sys.exit(1)


def sigint_handler(_, __):
    print('Exiting...')
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)

if __name__ == '__main__':
    main()
