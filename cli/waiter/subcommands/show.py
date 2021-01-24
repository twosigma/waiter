import collections

from tabulate import tabulate

from waiter import terminal
from waiter.data_format import display_data
from waiter.format import format_field_name, format_last_request_time, format_mem_field, format_memory_amount, \
    format_status, format_timestamp_string

from waiter.querying import print_no_data, query_token
from waiter.util import guard_no_cluster, is_service_current


def format_using_current_token(service, token_etag, token_name):
    """Formats the "Current?" column for the given service"""
    is_current = is_service_current(service, token_etag, token_name)
    if is_current:
        return terminal.success('Current')
    else:
        return 'Not Current'


def retrieve_num_instances(service):
    """Returns the total number of instances."""
    instance_counts = service["instance-counts"]
    return instance_counts["healthy-instances"] + instance_counts["unhealthy-instances"]


def tabulate_token_services(services, token_etag, token_name):
    """Returns a table displaying the service info"""
    num_services = len(services)
    if num_services > 0:
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

        services = sorted(services, key=lambda s: s.get('last-request-time', None) or '', reverse=True)
        rows = [collections.OrderedDict([('Service Id', s['service-id']),
                                         ('Run as user', s['effective-parameters']['run-as-user']),
                                         ('Instances', retrieve_num_instances(s)),
                                         ('CPUs', s['effective-parameters']['cpus']),
                                         ('Memory', format_mem_field(s['effective-parameters'])),
                                         ('Version', s['effective-parameters']['version']),
                                         ('Status', format_status(s['status'])),
                                         ('Last request', format_last_request_time(s)),
                                         ('Current?', format_using_current_token(s, token_etag, token_name))])
                for s in services]
        service_table = tabulate(rows, headers='keys', tablefmt='plain')
        return f'\n\n{summary_table}\n\n{service_table}'
    else:
        return ''


def tabulate_token(cluster_name, token, token_name, services, token_etag):
    """Given a token, returns a string containing tables for the fields"""
    table = [['Owner', token['owner']]]
    if token.get('name'):
        table.append(['Name', token['name']])
    if token.get('cpus'):
        table.append(['CPUs', token['cpus']])
    if token.get('mem'):
        table.append(['Memory', format_mem_field(token)])
    if token.get('ports'):
        table.append(['Ports requested', token['ports']])
    if token.get('cmd-type'):
        table.append(['Command type', token['cmd-type']])
    if token.get('health-check-url'):
        table.append(['Health check endpoint', token['health-check-url']])
    if token.get('permitted-user'):
        table.append(['Permitted user(s)', token['permitted-user']])

    explicit_keys = ('cmd', 'cmd-type', 'cpus', 'env', 'health-check-url', 'last-update-time',
                     'last-update-user', 'mem', 'name', 'owner', 'permitted-user', 'ports')
    ignored_keys = ('cluster', 'previous', 'root')
    for key, value in token.items():
        if key not in (explicit_keys + ignored_keys):
            table.append([format_field_name(key), value])

    command = token.get('cmd')
    if command:
        token_command = f'Command:\n{command}'
    else:
        token_command = '<No command specified>'

    if token.get('env') and len(token['env']) > 0:
        environment = '\n\nEnvironment:\n%s' % '\n'.join(['%s=%s' % (k, v) for k, v in token['env'].items()])
    else:
        environment = ''

    table_text = tabulate(table, tablefmt='plain')
    last_update_time = format_timestamp_string(token['last-update-time'])
    last_update_user = f' ({token["last-update-user"]})' if 'last-update-user' in token else ''
    service_table = tabulate_token_services(services, token_etag, token_name)
    return f'\n' \
        f'=== {terminal.bold(cluster_name)} / {terminal.bold(token_name)} ===\n' \
        f'\n' \
        f'Last Updated: {last_update_time}{last_update_user}\n' \
        f'\n' \
        f'{table_text}\n' \
        f'\n' \
        f'{token_command}' \
        f'{environment}' \
        f'{service_table}'


def show(clusters, args, _, __):
    """Prints info for the token with the given token name."""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    as_yaml = args.get('yaml')
    token_name = args.get('token')[0]
    include_services = not args.get('no-services')

    query_result = query_token(clusters, token_name, include_services=include_services)

    if as_json or as_yaml:
        display_data(args, query_result)
    else:
        for cluster_name, entities in sorted(query_result['clusters'].items()):
            services = entities['services'] if include_services else []
            print(tabulate_token(cluster_name, entities['token'], token_name, services, entities['etag']))
            print()

    if query_result['count'] > 0:
        return 0
    else:
        if not as_json and not as_yaml:
            print_no_data(clusters)
        return 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('show', help='show token by name')
    show_parser.add_argument('token', nargs=1)
    show_parser.add_argument('--no-services', help="don't show the token's services",
                             dest='no-services', action='store_true')
    format_group = show_parser.add_mutually_exclusive_group()
    format_group.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    format_group.add_argument('--yaml', help='show the data in YAML format', dest='yaml', action='store_true')
    return show
