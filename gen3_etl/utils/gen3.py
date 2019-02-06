"""Utility, helps with gen3."""
import os
import urllib3
import requests
import sys
import json
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3_etl.utils.collections import grouper

from requests.packages.urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
os.environ['CURL_CA_BUNDLE'] = ''

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
        response = json.loads(submission_client.submit_node(program_name, project_code, nodes))

        assert response['code'] == 200, 'could not create {} {}'.format(nodes[0]['type'], response)
        print('created {} {}(s)'.format(len(response['entities']), response['entities'][0]['type']), file=sys.stderr)
        return response
    except AssertionError as e:
        for entity in response['entities']:
            for error in entity.get('errors', []):
                print('ERROR {} {} {}'.format(error['type'], entity['type'], entity), file=sys.stderr)
                raise e
                # if error['type'] == 'INVALID_LINK':
                #     print('WARNING INVALID_LINK {} {}'.format(entity['type'],entity), file=sys.stderr)
                # else:
                #     print('ERROR {} {} {}'.format(error['type'], entity['type'],entity), file=sys.stderr)
                #     raise e


def delete_all(submission_client, program, project, batch_size=200, types=['submitted_methylation', 'aliquot', 'sample', 'demographic', 'case', 'experiment']):
    """Delete all nodes in types hierarchy."""
    for t in types:
        print(t)
        response = submission_client.export_node_all_type(program, project, t)
        if 'data' not in response:
            print('no data?', response, file=sys.stderr)
        else:
            for ids in grouper(batch_size, [n['node_id'] for n in response['data']]):
                len_ids = len(ids)
                ids = ','.join(ids)
                delete_response = submission_client.delete_node(program, project, ids)
                delete_response = json.loads(delete_response)
                assert delete_response['code'] == 200, delete_response
                print('deleted {} {}'.format(len_ids, t), file=sys.stderr)


def create_experiment(submission_client, program, project, submitter_id):
    """Create experiment."""
    experiment = {
        '*projects': {'code': project},
        '*submitter_id': submitter_id,
        'type': 'experiment'
    }
    return create_node(submission_client, program, project, experiment)
