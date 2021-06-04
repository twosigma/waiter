#!/usr/bin/env python3
import os

from setuptools import setup

requirements = [
    'arrow>=0.13.1',
    'humanfriendly>=4.18',
    'pyyaml>=3.13',
    'requests>=2.20.0',
    'tabulate>=0.8.3'
]

test_requirements = [
    'pytest',
    'retrying'
]

extras = {
    'test': test_requirements,
}


def get_version():
    this_file = os.path.dirname(os.path.abspath(__file__))
    version_file = os.path.join(this_file, 'waiter', 'version.py')
    with open(version_file, 'r') as f:
        version_string = f.read().strip().split('=')[1].strip().strip("'")
        print(f'waiter version is {version_string}')
        return version_string


setup(
    name='waiter_client',
    version=get_version(),
    description="Two Sigma's Waiter CLI",
    long_description="This package contains Two Sigma's Waiter command line interface, waiter. waiter allows you to "
                     "create/update/delete/view tokens and also view services across multiple Waiter clusters.",
    packages=['waiter', 'waiter.subcommands'],
    entry_points={'console_scripts': ['waiter = waiter.__main__:main']},
    install_requires=requirements,
    tests_require=test_requirements,
    extras_require=extras
)
