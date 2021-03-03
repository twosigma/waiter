import collections

from tabulate import tabulate

from waiter import terminal
from waiter.format import format_last_request_time, format_mem_field, format_memory_amount, format_status
from waiter.util import is_service_current, print_error


def retrieve_num_instances(service):
    """Returns the total number of instances."""
    instance_counts = service["instance-counts"]
    return instance_counts["healthy-instances"] + instance_counts["unhealthy-instances"]


def format_using_current_token(service, token_etag, token_name):
    """Formats the "Current?" column for the given service"""
    is_current = is_service_current(service, token_etag, token_name)
    if is_current:
        return terminal.success('Current')
    else:
        return 'Not Current'


def format_instance_status(instance):
    """Formats the "Healthy?" column for the given instance"""
    if instance['healthy?']:
        return terminal.success('Healthy')
    else:
        if instance['_status'] == 'failed':
            status = 'Failed'
        elif instance['_status'] == 'killed':
            status = 'Killed'
        else:
            status = 'Unhealthy'
        return terminal.failed(status)


def tabulate_token_services(services, token_name, token_etag=None, show_index=False, summary_table=True,
                            column_names=[]):
    """
    :param services: list of services to be displayed as rows
    :param token_name: the token that the services belong to
    :param token_etag: token_etag determines if a the service is current, will default to service['etag'] or None
    :param show_index: shows index column (usually for future choice prompt)
    :param summary_table: provides summary table of services (e.g. Total Memory, CPUS)
    :param column_names: column fields to be included in table
    :return: tabular output string and sorted services list in descending order by last request time
    """
    num_services = len(services)
    if num_services > 0:
        services = sorted(services, key=lambda s: s.get('last-request-time', None) or '', reverse=True)
        rows = [collections.OrderedDict([(key, data)
                                         for key, data in
                                         [('Index', f'[{index + 1}]'),
                                          ('Service Id', s['service-id']),
                                          ('Cluster', s.get('cluster', None)),
                                          ('Run as user', s['effective-parameters']['run-as-user']),
                                          ('Instances', retrieve_num_instances(s)),
                                          ('CPUs', s['effective-parameters']['cpus']),
                                          ('Memory', format_mem_field(s['effective-parameters'])),
                                          ('Version', s['effective-parameters']['version']),
                                          ('In-flight req.', s['request-metrics']['outstanding']),
                                          ('Status', format_status(s['status'])),
                                          ('Last request', format_last_request_time(s)),
                                          ('Current?',
                                           format_using_current_token(s, token_etag or s.get('etag', None),
                                                                      token_name))]
                                         if key in column_names or show_index and key == 'Index'])
                for index, s in enumerate(services)]
        service_table = tabulate(rows, headers='keys', tablefmt='plain')
        if summary_table:
            num_failing_services = len([s for s in services if s['status'] == 'Failing'])
            num_instances = sum(retrieve_num_instances(s) for s in services)
            total_mem_usage = format_memory_amount(sum(s['resource-usage']['mem'] for s in services))
            total_cpu_usage = round(sum(s['resource-usage']['cpus'] for s in services), 2)
            table = [['# Services', num_services],
                     ['# Failing', num_failing_services],
                     ['# Instances', num_instances],
                     ['Total Memory', total_mem_usage],
                     ['Total CPUs', total_cpu_usage]]
            summary_table = tabulate(table, tablefmt='plain')
            return f'\n\n{summary_table}\n\n{service_table}', services
        else:
            return service_table, services
    else:
        return '', services


def tabulate_service_instances(instances, show_index=False, column_names=[]):
    """
    :param instances: list of instances to be displayed
    :param show_index: shows index column (usually for future choice prompt)
    :param column_names: column names to be displayed in table
    :return: tabular output string
    """
    if len(instances) > 0:
        rows = [collections.OrderedDict([(key, data)
                                         for key, data in
                                         [('Index', f'[{index + 1}]'),
                                          ('Instance Id', inst['id']),
                                          ('Host', inst['host']),
                                          ('Status', format_instance_status(inst))]
                                         if key in column_names or show_index and key == 'Index'])
                for index, inst in enumerate(instances)]
        return tabulate(rows, headers='keys', tablefmt='plain')
    else:
        return ''


def get_user_selection(items, tabular_str, short_circuit_choice=True):
    """
    :param items: list of possible choices
    :param tabular_str: table output to provide user with options
    :param short_circuit_choice: When True and only one item in items, return that item as the selection without user
    prompt.
    :exception Raises exception when user input is invalid
    :return selected item (an element from the items list)
    """
    if short_circuit_choice and len(items) == 1:
        return items[0]
    print(tabular_str)
    answer = input(f'Enter the Index of your choice: ')
    print()
    try:
        index = int(answer) - 1
        if index < 0 or index >= len(items):
            raise Exception('Input is out of range!')
        return items[index]
    except ValueError as error:
        print_error('Input received did not match any of the choices!')
        raise error
