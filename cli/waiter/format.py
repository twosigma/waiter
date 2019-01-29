import humanfriendly


def format_memory_amount(megabytes):
    """Formats an amount, in MB, to be human-readable"""
    return humanfriendly.format_size(megabytes * 1000 * 1000)


def format_mem_field(job):
    """Formats the job memory field"""
    return format_memory_amount(job['mem'])
