import json
import logging
import os
import sys

import yaml


class DataFormat:
    def __str__(self):
        """Returns the name of the format in upper case."""
        return self.name().upper()

    def name(self):
        """Returns the name of the format."""
        raise NotImplementedError('Method has not been implemented!')

    def parse(self, data):
        """Returns the parsed data."""
        raise NotImplementedError('Method has not been implemented!')

    def dump(self, out_data, out_file=None):
        """
        If the file is provided, writes the string representation of the data to the file.
        Else it returns the string representation of the data in the format.
        """
        raise NotImplementedError('Method has not been implemented!')


class JsonDataFormat(DataFormat):
    def name(self):
        return 'json'

    def parse(self, data):
        try:
            logging.debug(f'parsing input data as json')
            content = json.loads(data)
            return content
        except Exception:
            raise ValueError('Malformed JSON in input.')

    def dump(self, out_data, out_file=None):
        if out_file:
            json.dump(out_data, out_file, indent=2, sort_keys=True)
        else:
            return json.dumps(out_data, indent=2, sort_keys=True)


class YamlDataFormat(DataFormat):
    def name(self):
        return 'yaml'

    def parse(self, data):
        try:
            logging.debug(f'parsing input data as yaml')
            content = yaml.safe_load(data)
            return content
        except Exception:
            raise ValueError('Malformed YAML in input.')

    def dump(self, out_data, out_file=None):
        if out_file:
            yaml.safe_dump(out_data, out_file)
        else:
            return yaml.safe_dump(out_data)


JSON = JsonDataFormat()
YAML = YamlDataFormat()


class AnySupportedFormat(DataFormat):
    def name(self):
        return 'data'

    def parse(self, data):
        content = None
        for input_format in [JSON, YAML]:
            try:
                logging.debug(f'attempting to parse input data as {input_format}')
                content = input_format.parse(data)
                logging.debug(f'successfully parsed input data as {input_format}')
                continue
            except Exception:
                logging.debug(f'error parsing input data as {input_format}')
        if content is None:
            raise ValueError('Malformed data in input.')
        return content

    def dump(self, out_data, out_file=None):
        raise NotImplementedError('Data format needs to be specified explicitly!')


ANY_FORMAT = AnySupportedFormat()


def validate_options(options):
    """Validates whether unique file format is specified."""
    as_json = options.get(JSON.name())
    as_yaml = options.get(YAML.name())
    if as_json and as_yaml:
        raise Exception(f'JSON and YAML mode cannot be used simultaneously!')


def determine_format(options):
    """
    Determines whether the configured format is YAML or JSON.
    JSON is the default format if YAML is not explicitly enabled.
    """
    validate_options(options)
    as_yaml = options.get(YAML.name())
    input_format = YAML if as_yaml else JSON
    return input_format


def read_from_standard_input(input_format):
    """Prompts for and then reads token JSON from stdin"""
    logging.debug(f'expecting {input_format} input from stdin')
    print(f'Enter the raw {input_format} (press Ctrl+D on a blank line to submit)', file=sys.stderr)
    data = sys.stdin.read()
    return data


def load_file(path):
    """Loads the file"""
    content = None

    if os.path.isfile(path):
        with open(path) as data_file:
            try:
                logging.debug(f'attempting to load content from {path}')
                content = data_file.read()
            except Exception:
                logging.exception(f'encountered exception when loading content from {path}')
    else:
        logging.info(f'{path} is not a file')

    return content


def load_data(options):
    """
    Decode a JSON/YAML formatted file.
    Data must be a dict of attributes.
    Throws a ValueError if there is a problem parsing the data.
    """
    input_format = ANY_FORMAT if options.get('data') else determine_format(options)
    input_file = options.get(input_format.name())

    if input_file == '-':
        logging.debug(f'reading {input_format} input from stdin')
        content = read_from_standard_input(input_format)
        if not content:
            raise Exception(f'Unable to load {input_format} from stdin.')
    else:
        logging.debug(f'reading {input_format} input from {input_file}')
        content = load_file(input_file)
        if not content:
            raise Exception(f'Unable to load {input_format} from {input_file}.')

    content = input_format.parse(content)
    if type(content) is dict:
        return content
    else:
        raise ValueError(f'Input {input_format} must be a dictionary of attributes.')


def display_data(options, data):
    """Display data as JSON/YAML format to standard output."""
    input_format = determine_format(options)
    result = input_format.dump(data)
    if result:
        print(result)
