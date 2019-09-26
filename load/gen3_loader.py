"""Utility, creates projects and nodes.  Deletes existing nodes of input type by default."""
import os
from glob import glob
import json
import sys
import multiprocessing as mp

from gen3_etl.utils.ioutils import reader
from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.gen3 import create_node, delete_all, submission_client
from gen3_etl.utils.collections import grouper

DEFAULT_INPUT_DIR = 'output'
DEFAULT_PROGRAM = 'smmart'
DEFAULT_PROJECT = None
DEFAULT_PATH = '**/*.json*'
DEFAULT_BATCH_SIZE = 100
DEFAULT_DELETE_FIRST = False

DEFAULT_CREDENTIALS_PATH = os.path.join('config', 'credentials.json')
DEFAULT_ENDPOINT = 'https://localhost'


def upload(path, program, project, submission_client, batch_size, delete_first):
    """Read gen3 json and write to gen3."""
    pool = mp.Pool(mp.cpu_count())

    def collect_result(upload_response):
        pass

    for p in glob(path):
        deleted = False
        for lines in grouper(batch_size, reader(p)):
            nodes = [l for l in lines]

            if nodes[0]['type'] == 'project':
                for node in nodes:
                    r = submission_client.create_project(program, node)
                    response = None
                    try:
                        response = json.loads(r)
                    except Exception as e:
                        pass
                    assert response, 'could not parse response {}'.format(r)
                    assert response['code'] == 200, 'could not create {} {}'.format(nodes[0]['type'], response)
                    assert 'successful' in response['message'], 'could not create {} {}'.format(nodes[0]['type'], response)
                    print('Created project {}'.format(node['code']), file=sys.stderr)
                continue

            if nodes[0]['type'] == 'experiment':
                project = nodes[0]['projects'][0]['code']

            if not deleted and delete_first:
                delete_all(submission_client, program, project, types=[nodes[0]['type']])
                deleted = True

            pool.apply_async(create_node, args=(submission_client, program, project, nodes), callback=collect_result)

    print(f'pool.close/join')
    pool.close()
    pool.join()  # postpones the execution of next line of code until all processes in the queue are done



if __name__ == "__main__":
    parser = default_argument_parser(
        description='Reads gen3 json ({}).'.format(DEFAULT_INPUT_DIR)
    )
    parser.add_argument('--path', type=str,
                        default=DEFAULT_PATH,
                        help='Match these files ({}).'.format(DEFAULT_PATH))
    parser.add_argument('--program', type=str,
                        default=DEFAULT_PROGRAM,
                        help='Name of existing gen3 program ({}).'.format(DEFAULT_PROGRAM))
    parser.add_argument('--project', type=str,
                        default=DEFAULT_PROJECT,
                        required=True,
                        help='Name of existing gen3 project ({}).'.format(DEFAULT_PROJECT))

    parser.add_argument('--batch_size', type=int,
                        default=DEFAULT_BATCH_SIZE,
                        help='Number of records to send to gen3 at a time({}).'.format(DEFAULT_BATCH_SIZE))

    parser.add_argument('--delete_first', type=bool,
                        default=DEFAULT_DELETE_FIRST,
                        help='Delete all types from project before upload?({}).'.format(DEFAULT_DELETE_FIRST))

    parser.add_argument('--credentials_path', type=str,
                        default=DEFAULT_CREDENTIALS_PATH,
                        help='Location of gen3 path ({}).'.format(DEFAULT_CREDENTIALS_PATH))

    parser.add_argument('--endpoint', type=str,
                        default=DEFAULT_ENDPOINT,
                        help='gen3 host base url ({}).'.format(DEFAULT_ENDPOINT))

    args = parser.parse_args()

    sc = submission_client(refresh_file=args.credentials_path, endpoint=args.endpoint)

    upload(path=args.path,
           program=args.program,
           project=args.project,
           submission_client=sc,
           batch_size=args.batch_size,
           delete_first=args.delete_first,
           )
