import arrow
import humanfriendly

from waiter import terminal


def format_memory_amount(mebibytes):
    """Formats an amount, in MiB, to be human-readable"""
    return humanfriendly.format_size(mebibytes * 1024 * 1024, binary=True)


def format_mem_field(job):
    """Formats the job memory field"""
    return format_memory_amount(job['mem'])


def format_timestamp_string(s):
    """Formats the given timestamp string in the "time ago" format"""
    return arrow.get(s).humanize()


def format_field_name(s):
    """Formats the given field name in a more readable format"""
    parts = s.split('-')
    if parts[-1] == 'secs':
        parts[-1] = '(seconds)'
    if parts[0] == 'min':
        parts[0] = 'minimum'
    elif parts[0] == 'max':
        parts[0] = 'maximum'
    with_spaces = ' '.join(parts)
    return with_spaces.capitalize()


def format_status(status):
    """Formats service status"""
    if status == 'Running':
        return terminal.running(status)
    elif status == 'Inactive':
        return terminal.inactive(status)
    elif status == 'Failing':
        return terminal.failed(status)
    elif status == 'Starting':
        return terminal.starting(status)
    else:
        return status


def format_last_request_time(service):
    """Formats the last request time of the given service"""
    if 'last-request-time' in service and service['last-request-time']:
        last_request_time = format_timestamp_string(service['last-request-time'])
    else:
        last_request_time = 'n/a'
    return last_request_time
