#!/usr/bin/env python3

from setuptools import setup

from waiter import version

requirements = [
    'humanfriendly',
    'requests',
    'tabulate'
]

test_requirements = [
    'pytest',
    'retrying'
]

extras = {
    'test': test_requirements,
}

setup(
    name='waiter_client',
    version=version.VERSION,
    description="Two Sigma's Waiter CLI",
    long_description="This package contains Two Sigma's Waiter command line interface, waiter. waiter allows you to "
                     "create/update/delete tokens and view tokens and services across multiple Waiter clusters.",
    packages=['waiter', 'waiter.subcommands'],
    entry_points={'console_scripts': ['waiter = waiter.__main__:main']},
    install_requires=requirements,
    tests_require=test_requirements,
    extras_require=extras
)
