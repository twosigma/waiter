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
    params = {'effective-parameters': 'true',
              'token': token_name}
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


def _get_latest_cluster(clusters, query_result):
    """
    :param clusters: list of local cluster configs from the configuration file
    :param query_result: value from query_token function
    :return: Finds latest token configuration from the query_result. Gets the cluster that is configured in the
     token description and returns a local cluster who's serverside name matches the one specified in the token.
     If the token's cluster does not exist in one of the local cluster configurations then an Exception is raised.
    """
    token_descriptions = list(query_result['clusters'].values())
    token_result = max(token_descriptions, key=lambda token: token['token']['last-update-time'])
    cluster_name_goal = token_result['token']['cluster']
    provided_cluster_names = []
    for c in clusters:
        cluster_settings, _ = http_util.make_data_request(c, lambda: http_util.get(c, '/settings'))
        cluster_config_name = cluster_settings['cluster-config']['name']
        provided_cluster_names.append(cluster_config_name)
        if cluster_name_goal.upper() == cluster_config_name.upper():
            return c
    raise Exception(f'The token is configured in cluster {cluster_name_goal}, which is not provided.' +
                    f' The following clusters were provided: {", ".join(provided_cluster_names)}.')


def get_target_cluster_from_token(clusters, token_name, enforce_cluster):
    """
    :param clusters: list of local cluster configs from the configuration file
    :param token_name: string name of token
    :param enforce_cluster: boolean describing if cluster was explicitly specified as an cli argument
    :return: Return the target cluster config for various token operations
    """
    query_result = query_token(clusters, token_name)
    if query_result["count"] == 0:
        raise Exception('The token does not exist. You must create it first.')
    elif enforce_cluster:
        logging.debug(f'Forcing cluster {clusters[0]} as the target_cluster')
        return clusters[0]
    else:
        sync_group_count = 0
        sync_groups_set = set()
        cluster_names = set()
        for cluster in list(query_result['clusters'].keys()):
            cluster_config = next(c for c in clusters if c['name'] == cluster)
            sync_group = cluster_config.get('sync-group', False)
            # consider clusters that don't have a configured sync-group as in their own unique group
            if not sync_group:
                sync_group = sync_group_count
                sync_group_count += 1
            sync_groups_set.add(sync_group)
            cluster_names.add(cluster)
        if len(sync_groups_set) > 1:
            raise Exception('Could not infer the target cluster for this operation because there are multiple cluster '
                            f'groups that contain a description for this token: groups-{sync_groups_set} '
                            f'clusters-{cluster_names}.'
                            '\nConsider specifying with the --cluster flag which cluster you are targeting.')
        return _get_latest_cluster(clusters, query_result)
