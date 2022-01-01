import logging

from waiter import http_util, terminal
from waiter.querying import get_services_using_token, get_target_cluster_from_token, print_no_data, query_token
from waiter.util import get_in, guard_no_cluster, response_message, print_error, str2bool


def delete_token_on_cluster(cluster, token_name, token_etag):
    """Deletes the token with the given token name in the given cluster."""
    cluster_name = cluster['name']
    try:
        # Retrieve all services that are using the token
        services = get_services_using_token(cluster, token_name)
        if services is not None:
            num_services = len(services)
            if num_services > 0:
                if num_services == 1:
                    print_error(f'There is one service using token {token_name} in {cluster_name}:\n')
                else:
                    print_error(f'There are {num_services} services using token {token_name} in {cluster_name}:\n')
                for service in services:
                    print_error(f'  {service["url"]}')
                if num_services == 1:
                    print_error('\nPlease kill this service before deleting the token it relies on.')
                else:
                    print_error('\nPlease kill these services before deleting the token they rely on.')
                return False
        else:
            print_error(f'Unable to retrieve services using token {token_name} in {cluster_name}.')
            return False

        # Delete the token
        print(f'Deleting token {terminal.bold(token_name)} in {terminal.bold(cluster_name)}...')
        headers = {
            'If-Match': token_etag,
            'X-Waiter-Token': token_name
        }
        resp = http_util.delete(cluster, '/token', headers=headers)
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            print(f'Successfully deleted {token_name} in {cluster_name}.')
            return True
        else:
            print_error(response_message(resp.json()))
            return False
    except Exception as e:
        message = f'Encountered error while deleting token {token_name} in {cluster_name}.'
        logging.error(e, message)
        print_error(message)


def delete(clusters, args, _, enforce_cluster):
    """Deletes the token with the given token name."""
    guard_no_cluster(clusters)
    force_delete = args.get('force', False)
    token_name = args.get('token')[0]
    query_result = query_token(clusters, token_name)
    if query_result['count'] == 0:
        print_no_data(clusters)
        return 1

    cluster_data_pairs = sorted(query_result['clusters'].items())
    num_clusters = len(cluster_data_pairs)
    clusters_by_name = {c['name']: c for c in clusters}
    sync_opt_out = any(get_in(token_config, ['token', 'metadata', 'waiter-token-sync-opt-out']) == 'true'
                       for _, token_config in cluster_data_pairs)

    if force_delete or sync_opt_out:
        cluster_data_pairs = sorted(query_result['clusters'].items())
        clusters_by_name = {c['name']: c for c in clusters}
        num_clusters = len(cluster_data_pairs)
        cluster_names_found = [p[0] for p in cluster_data_pairs]

        print(f'Token {terminal.bold(token_name)} exists in {num_clusters} cluster(s): {", ".join(cluster_names_found)}.')
        overall_success = True
        for cluster_name, data in cluster_data_pairs:
            if force_delete or (num_clusters == 1):
                should_delete = True
            else:
                should_delete = str2bool(input(f'Delete token in {terminal.bold(cluster_name)}? '))
            if should_delete:
                cluster = clusters_by_name[cluster_name]
                success = delete_token_on_cluster(cluster, token_name, data['etag'])
                overall_success = overall_success and success
    else:
        cluster = get_target_cluster_from_token(clusters, token_name, enforce_cluster)
        if cluster is None:
            print_no_data(clusters)
            return 1
        else:
            token_config = query_result['clusters'][cluster['name']]
            token_etag = token_config['etag']
            success = delete_token_on_cluster(cluster, token_name, token_etag)
            return 0 if success else 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('delete', help='delete token by name')
    parser.add_argument('token', nargs=1)
    parser.add_argument('--force', '-f', help='delete on all clusters where present, never prompt',
                        dest='force', action='store_true')
    return delete
