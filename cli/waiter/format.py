import arrow
import humanfriendly


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
