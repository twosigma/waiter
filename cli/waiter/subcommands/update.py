from functools import partial

from waiter import token_post

parser = None


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    global parser
    action = token_post.Action.UPDATE
    parser = token_post.register_argument_parser(add_parser, action)
    token_post.add_arguments(parser)
    return partial(token_post.create_or_update_token, action=action)


def add_implicit_arguments(unknown_args):
    """
    Given the list of "unknown" args, dynamically adds proper arguments to
    the subparser, allowing us to support any token parameter as a flag
    """
    token_post.add_implicit_arguments(unknown_args, parser)
