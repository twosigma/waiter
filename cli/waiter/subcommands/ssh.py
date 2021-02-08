import argparse
import os

def ssh(clusters, args, _, __):
    # should I even allow ssh into non kubernetes cluster?
    # not on kubernetes ssh in marathon
    os.execlp('kubectl', 'kubectl',
              'exec',
              '-c', os.getenv('COOK_CONTAINER_NAME_FOR_JOB', 'required-cook-job-container'),
              '-it', instance_uuid,
              '--', '/bin/sh', '-c', 'cd $HOME; exec /bin/sh')

    return 0


def register(add_parser):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('ssh',
                        help='ssh to a pod given the token, service-id, or pod name. Only kubernetes is supported.')
    parser.add_argument('token-or-service-id-or-pod-name')
    id_group = parser.add_mutually_exclusive_group(required=False)
    id_group.add_argument('--token', '-t', dest='is-token', action='store_true')
    id_group.add_argument('--service-id', '-s', dest='is-service-id', action='store_true')
    id_group.add_argument('--pod-name', '-p', dest='is-pod-name', action='store_true')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    return ssh
