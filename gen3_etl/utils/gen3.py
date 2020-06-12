"""Utility, helps with gen3."""
import os
import urllib3
import requests
import sys
import json
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3_etl.utils.collections import grouper
import logging
import hashlib
import multiprocessing as mp


from requests.packages.urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
os.environ['CURL_CA_BUNDLE'] = ''

logger = logging.getLogger('utils.gen3')


DEFAULT_CREDENTIALS_PATH = os.path.join('config', 'credentials.json')
DEFAULT_HOST = 'localhost'
DEFAULT_ENDPOINT = 'https://{}'.format(DEFAULT_HOST)


def submission_client(endpoint=DEFAULT_ENDPOINT, refresh_file=DEFAULT_CREDENTIALS_PATH):
    """Create authorized client."""
    auth = Gen3Auth(endpoint, refresh_file=refresh_file)
    assert auth, 'should return an auth client'
    submission_client = Gen3Submission(endpoint, auth)
    assert submission_client, 'should return a submission client'
    assert 'delete_program' in dir(submission_client), 'should have a delete_program method'
    assert 'create_program' in dir(submission_client), 'should have a create_program method'
    return submission_client


def create_node(submission_client, program_name, project_code, node):
    """Create node(s)."""
    try:
        nodes = node
        if not isinstance(node, (list,)):
            nodes = [node]
        response_text = None    
        response = None
        response = submission_client.submit_record(program_name, project_code, nodes)
        # response = json.loads(response_text)
        # logger.info(f"create_node: status_code:{response['code']}")
        assert response['code'] == 200, 'could not create {} {}'.format(nodes[0]['type'], response_text)
        logger.info('created {} {}(s)'.format(len(response['entities']), response['entities'][0]['type']))
        return response
    except Exception as e:
        logger.error(f"create_node: error {e}")
        logger.error(f"create_node: error {response_text} {nodes}")
        if response:
            for entity in response.get('entities', []):
                for error in entity.get('errors', []):
                    logger.error('{} {} {}'.format(error['type'], entity['type'], entity))
            for error in response.get('transactional_errors', []):
                logger.error(' transactional_error {}'.format(error))
                logger.error(json.dumps(response))
        raise e
                # if error['type'] == 'INVALID_LINK':
                #     print('WARNING INVALID_LINK {} {}'.format(entity['type'],entity), file=sys.stderr)
                # else:
                #     print('ERROR {} {} {}'.format(error['type'], entity['type'],entity), file=sys.stderr)
                #     raise e

def delete_type(submission_client, program, project, batch_size, t):
    response = submission_client.export_node(program, project, node_type=t, fileformat='json')
    # # pool = mp.Pool(mp.cpu_count())

    def collect_result(delete_response):
        delete_response = delete_response.json()
        assert delete_response['code'] == 200, delete_response
        logger.info('deleted {} {}'.format(t, delete_response['message']))

    if 'data' not in response or len(response['data']) == 0:
        logger.warning(f'No {t} to delete {response}')
    else:
        for ids in grouper(batch_size, [n['node_id'] for n in response['data']]):
            logger.info(f'deleting {len(ids)}')
            ids = ','.join(ids)
            collect_result(submission_client.delete_record(program, project, ids))
            # # pool.apply_async(submission_client.delete_record, args=(program, project, ids), callback=collect_result)
        # Close Pool and let all the processes complete
        # postpones the execution of next line of code until all processes in the queue are done
        # # pool.close()
        # # pool.join()

def delete_all(submission_client, program, project, batch_size=200, types=['submitted_methylation', 'aliquot', 'sample', 'demographic', 'case', 'experiment']):
    """Delete all nodes in types hierarchy."""
    for t in types:
        print('{}-{}.{}'.format(program, project, t))
        try:
            delete_type(submission_client, program, project, batch_size, t)
        except Exception as e:
            print(e)


def create_experiment(submission_client, program, project, submitter_id):
    """Create experiment."""
    experiment = {
        '*projects': {'code': project},
        '*submitter_id': submitter_id,
        'type': 'experiment'
    }
    return create_node(submission_client, program, project, experiment)


# https://github.com/uc-cdis/gdcdatamodel/blob/develop/gdcdatamodel/models/__init__.py#L163
def get_class_tablename_from_id(_id):
    return 'node_{}'.format(_id.replace('_', ''))


# https://github.com/uc-cdis/gdcdatamodel/blob/develop/gdcdatamodel/models/__init__.py#L370
def generate_edge_tablename(src_label, label, dst_label):
    """Generate a name for the edge table.
    Because of the limit on table name length on PostgreSQL, we have
    to truncate some of the longer names.  To do this we concatenate
    the first 2 characters of each word in each of the input arguments
    up to 10 characters (per argument).  However, this strategy would
    very likely lead to collisions in naming.  Therefore, we take the
    first 8 characters of a hash of the full, un-truncated name
    *before* we truncate and prepend this to the truncation.  This
    gets us a name like ``edge_721d393f_LaLeSeqDaFrLaLeSeBu``.  This
    is rather an undesirable workaround. - jsm
    """

    tablename = 'edge_{}{}{}'.format(
        src_label.replace('_', ''),
        label.replace('_', ''),
        dst_label.replace('_', ''),
    )

    # If the name is too long, prepend it with the first 8 hex of it's hash
    # truncate the each part of the name
    if len(tablename) > 40:
        oldname = tablename
        logger.debug('Edge tablename {} too long, shortening'.format(oldname))
        tablename = 'edge_{}_{}'.format(
            str(hashlib.md5(tablename.encode('utf-8')).hexdigest())[:8],
            "{}{}{}".format(
                ''.join([a[:2] for a in src_label.split('_')])[:10],
                ''.join([a[:2] for a in label.split('_')])[:7],
                ''.join([a[:2] for a in dst_label.split('_')])[:10],
            )
        )
        logger.debug('Shortening {} -> {}'.format(oldname, tablename))

    return tablename
