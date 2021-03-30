import sys

import textwrap


MOVE_UP = '\033[F'


class Color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def failed(s):
    return colorize(s, Color.BOLD + Color.RED)


def success(s):
    return colorize(s, Color.GREEN)


def running(s):
    return colorize(s, Color.CYAN)


def inactive(s):
    return colorize(s, Color.YELLOW)


def starting(s):
    return colorize(s, Color.BLUE)


def reason(s):
    return colorize(s, Color.RED)


def bold(s):
    return colorize(s, Color.BOLD)


wrap = textwrap.wrap


def colorize(s, color):
    """Formats the given string with the given color"""
    return color + s + Color.END if tty() else s


def tty():
    """Returns true if running in a real terminal (as opposed to being piped or redirected)"""
    return sys.stdout.isatty()
