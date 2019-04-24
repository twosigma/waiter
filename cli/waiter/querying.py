import concurrent
import logging
import os
from concurrent import futures

from waiter import http_util, terminal


def query_across_clusters(clusters, query_fn):
    """Attempts to query entities from the given clusters."""
    count = 0
    all_entities = {'clusters': {}}
    max_workers = os.cpu_count()
    logging.debug('querying with max workers = %s' % max_workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_cluster = {query_fn(c, executor): c for c in clusters}
        for future, cluster in future_to_cluster.items():
            entities = future.result()
            cluster_count = entities['count']
            if cluster_count > 0:
                all_entities['clusters'][cluster['name']] = entities
                count += cluster_count
    all_entities['count'] = count
    return all_entities


def get_token(cluster, token_name, include=None):
    """Gets the token with the given name from the given cluster"""
    params = {'token': token_name}
    if include:
        params['include'] = include
    token_data, headers = http_util.make_data_request(cluster, lambda: http_util.get(cluster, 'token', params=params))
    etag = headers.get('ETag', None)
    return token_data, etag


def no_data_message(clusters):
    """Returns a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    message = terminal.failed(f'No matching data found in {clusters_text}.')
    message = f'{message}\nDo you need to add another cluster to your configuration?'
    return message


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    print(no_data_message(clusters))


def get_token_on_cluster(cluster, token_name, include_services=False):
    """Gets the token with the given name on the given cluster"""
    token_data, token_etag = get_token(cluster, token_name, include='metadata')
    if token_data:
        data = {'count': 1, 'token': token_data, 'etag': token_etag}
        if include_services:
            data['services'] = get_services_using_token(cluster, token_name)
        return data
    else:
        logging.info(f'Unable to retrieve token information on {cluster["name"]} ({cluster["url"]}).')
        return {'count': 0}


def query_token(clusters, token, include_services=False):
    """
    Uses query_across_clusters to make the token
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(get_token_on_cluster, cluster, token, include_services)

    return query_across_clusters(clusters, submit)


def get_service(cluster, service_id):
    """Retrieves the service with the given service id"""
    params = {'effective-parameters': True}
    endpoint = f'/apps/{service_id}'
    service, _ = http_util.make_data_request(cluster, lambda: http_util.get(cluster, endpoint, params=params))
    return service


def get_service_on_cluster(cluster, service_id):
    """Gets the service with the given service id on the given cluster"""
    service = get_service(cluster, service_id)
    if service:
        return {'count': 1, 'service': service}
    else:
        return {'count': 0}
                     
                     
def get_services_using_token(cluster, token_name):
    """Retrieves all services that are using the token"""
    params = {'token': token_name}
    services, _ = http_util.make_data_request(cluster, lambda: http_util.get(cluster, 'apps', params=params))
    return services


def get_services_on_cluster(cluster, token_name):
    """Gets the service(s) using the given token name on the given cluster"""
    services = get_services_using_token(cluster, token_name)
    if services:
        return {'count': len(services), 'services': services}
    else:
        return {'count': 0}


def query_service(clusters, service_id):
    """
    Uses query_across_clusters to make the service
    requests in parallel across the given clusters
    """
    return query_across_clusters(
        clusters,
        lambda cluster, executor: executor.submit(get_service_on_cluster, cluster, service_id))


def query_services(clusters, token_name):
    """
    Uses query_across_clusters to make the service
    requests in parallel across the given clusters
    """
    return query_across_clusters(
        clusters,
        lambda cluster, executor: executor.submit(get_services_on_cluster, cluster, token_name))


def get_tokens(cluster, user):
    """Gets the tokens owned by the given user from the given cluster"""
    params = {'owner': user, 'include': 'metadata'}
    tokens, _ = http_util.make_data_request(cluster, lambda: http_util.get(cluster, 'tokens', params=params))
    return tokens


def get_tokens_on_cluster(cluster, user):
    """Gets the tokens owned by the given user on the given cluster"""
    tokens = get_tokens(cluster, user)
    if tokens:
        data = {'count': len(tokens), 'tokens': tokens}
        return data
    else:
        logging.info(f'Unable to retrieve token information on {cluster["name"]} ({cluster["url"]}).')
        return {'count': 0}


def query_tokens(clusters, user):
    """
    Uses query_across_clusters to make the token
    requests in parallel across the given clusters
    """
    return query_across_clusters(
        clusters,
        lambda cluster, executor: executor.submit(get_tokens_on_cluster, cluster, user))
