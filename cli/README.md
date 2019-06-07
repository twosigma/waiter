## Waiter CLI

### Installation

To install the Waiter CLI, clone this repo and from this folder run:

```bash
pip3 install -e .
```

This will install the `waiter` command on your system.

### Configuration

In order to use the Waiter CLI, you'll need a configuration file. 
`waiter` looks first for a `.waiter.json` file in the current directory, and then for a `.waiter.json` file in your home directory. 
The path to this file may also be provided manually via the command line with the `--config` option.

There is a sample `.waiter.json` file included in this directory, which looks something like this:

```json
{
  "clusters": [
    {
      "name": "dev0",
      "url": "http://127.0.0.1:9091/",
      "disabled": false
    },
    {
      "name": "dev1",
      "url": "http://127.0.0.1:22321/",
      "disabled": true
    }
  ]
}
```

Each entry in the `clusters` array conforms to a cluster specification ("spec"). 
A cluster spec requires a name and a url pointing to a Waiter cluster.

### Commands

The fastest way to learn more about `waiter` is with the `-h` (or `--help`) option.

All global options (`--cluster`, `--config`, etc.) can be provided when using subcommands.

- `create`: You can create a token with `create`. 
- `show`: You can view a token's details with `show`.

### Publishing to PyPi

Use the following commands to publish the CLI to PyPi (https://pypi.org/project/waiter-client/):

```bash
# Run from the cli directory
$ rm -rf dist/ && python3 setup.py sdist bdist_wheel
$ python3 -m twine upload dist/*
```