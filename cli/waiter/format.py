import arrow
import humanfriendly


def format_memory_amount(megabytes):
    """Formats an amount, in MB, to be human-readable"""
    return humanfriendly.format_size(megabytes * 1000 * 1000)


def format_mem_field(job):
    """Formats the job memory field"""
    return format_memory_amount(job['mem'])


def format_timestamp_string(s):
    """Formats the given timestamp string in the "time ago" format"""
    return arrow.get(s).humanize()


def format_field_name(s):
    """Formats the given field name in a more readable format"""
    return s.capitalize().replace('-', ' ').replace('secs', '(seconds)')
