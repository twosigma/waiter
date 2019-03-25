import logging

from waiter import http_util, terminal
from waiter.querying import query_token, print_no_data
from waiter.util import guard_no_cluster, str2bool, response_message


def delete_token_on_cluster(cluster, token_name, token_etag):
    """TODO(DPO)"""
    cluster_name = cluster['name']
    try:
        # Retrieve all services that are using the token
        resp = http_util.get(cluster, '/apps', params={'token': token_name})
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            services = resp.json()
            # If there are any services, print an error and return
            num_services = len(services)
            if num_services > 0:
                if num_services == 1:
                    print(terminal.failed(
                        f'There is one service using token {token_name} in {cluster_name}:\n'))
                else:
                    print(terminal.failed(
                        f'There are {num_services} services using token {token_name} in {cluster_name}:\n'))
                for service in services:
                    print(f'  {service["url"]}')
                if num_services == 1:
                    print(terminal.failed('\nPlease kill this service before deleting the token it relies on.'))
                else:
                    print(terminal.failed('\nPlease kill these services before deleting the token they rely on.'))
                return
        else:
            print(terminal.failed(response_message(resp.json())))
            return

        # Delete the token
        print(f'Deleting token {terminal.bold(token_name)} in {terminal.bold(cluster_name)}...')
        headers = {
            'X-Waiter-Token': token_name,
            'If-Match': token_etag
        }
        resp = http_util.delete(cluster, '/token', headers=headers)
        logging.debug(f'Response status code: {resp.status_code}')
        if resp.status_code == 200:
            print(f'Successfully deleted {token_name} in {cluster_name}.')
        else:
            print(terminal.failed(response_message(resp.json())))
    except Exception as e:
        message = f'Encountered error while deleting token {token_name} in {cluster_name}.'
        logging.error(e, message)
        print(message)


def delete(clusters, args, _):
    """Deletes the token with the given token name."""
    guard_no_cluster(clusters)
    token_name = args.get('token')[0]
    query_result = query_token(clusters, token_name)
    cluster_data_pairs = sorted(query_result['clusters'].items())
    num_clusters = len(cluster_data_pairs)
    clusters_by_name = {c['name']: c for c in clusters}
    if num_clusters == 1:
        cluster_name = cluster_data_pairs[0][0]
        token_etag = cluster_data_pairs[0][1]['etag']
        cluster = clusters_by_name[cluster_name]
        delete_token_on_cluster(cluster, token_name, token_etag)
    elif num_clusters > 1:
        cluster_names_found = [p[0] for p in cluster_data_pairs]
        print(f'Token {terminal.bold(token_name)} exists in {num_clusters} clusters: {", ".join(cluster_names_found)}.')
        for cluster_name, data in cluster_data_pairs:
            should_delete = str2bool(input(f'Delete token in {terminal.bold(cluster_name)}? '))
            if should_delete:
                cluster = clusters_by_name[cluster_name]
                delete_token_on_cluster(cluster, token_name, data['etag'])

    if query_result['count'] > 0:
        return 0
    else:
        print_no_data(clusters)
        return 1


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('delete', help='delete token by name')
    parser.add_argument('token', nargs=1)
    return delete
