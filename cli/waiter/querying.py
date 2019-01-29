import concurrent
import logging
import os
from concurrent import futures

from waiter import http


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
            all_entities['clusters'][cluster['name']] = entities
            if entities.get('using_pools', False):
                for pool_entities in entities['pools'].values():
                    count += pool_entities['count']
            else:
                count += entities['count']
    all_entities['count'] = count
    return all_entities


def get_token(cluster, token_name):
    """Gets the token with the given name from the given cluster"""
    params = {'token': token_name}
    token = http.make_data_request(cluster, lambda: http.get(cluster, 'token', params=params))
    return token
