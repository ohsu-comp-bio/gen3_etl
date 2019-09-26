"""Utility, deletes projects and nodes.  Edit types array for your deployment."""
import os
import sys

from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.gen3 import delete_all, submission_client

DEFAULT_PROGRAM = None
DEFAULT_PROJECT = None
DEFAULT_CREDENTIALS_PATH = os.path.join('config', 'credentials.json')
DEFAULT_ENDPOINT = 'https://localhost'
DEFAULT_TYPES = ['submitted_methylation', 'submitted_somatic_mutation', 'read_group', 'demographic', 'aliquot', 'sample', 'bcc_diagnosis', 'diagnosis', 'bcc_demographic', 'demographic', 'bcc_participant', 'case', 'experiment']
DEFAULT_BATCH_SIZE = 100
DEFAULT_DROP_PROJECT = False


def delete(program, project, submission_client, types=DEFAULT_TYPES, batch_size=DEFAULT_BATCH_SIZE, drop_project=DEFAULT_DROP_PROJECT):
    """Delete all content from project."""
    delete_all(submission_client, program, project, types=types, batch_size=batch_size)
    try:
        if drop_project:
            print(submission_client.delete_project(program, project), file=sys.stderr)
    except Exception as e:
        print(e)
    # try:
    #     print(submission_client.delete_program(program), file=sys.stderr)
    # except Exception as e:
    #     print(e)


if __name__ == "__main__":
    parser = default_argument_parser(
        description='deletes all content for project'
    )
    parser.add_argument('--program', type=str,
                        default=DEFAULT_PROGRAM,
                        help='Name of existing gen3 program ({}).'.format(DEFAULT_PROGRAM))
    parser.add_argument('--project', type=str,
                        default=DEFAULT_PROJECT,
                        help='Name of existing gen3 project ({}).'.format(DEFAULT_PROJECT))
    parser.add_argument('--credentials_path', type=str,
                        default=DEFAULT_CREDENTIALS_PATH,
                        help='Location of gen3 path ({}).'.format(DEFAULT_CREDENTIALS_PATH))
    parser.add_argument('--endpoint', type=str,
                        default=DEFAULT_ENDPOINT,
                        help='gen3 host base url ({}).'.format(DEFAULT_ENDPOINT))
    parser.add_argument('--types', type=str,
                        default=DEFAULT_TYPES,
                        nargs='*',
                        help='list of entitites to delete ({}).'.format(DEFAULT_TYPES))
    parser.add_argument('--batch_size', type=int,
                        default=DEFAULT_BATCH_SIZE,
                        help='Number of records to send to gen3 at a time({}).'.format(DEFAULT_BATCH_SIZE))
    parser.add_argument('--drop_project', type=bool,
                        default=DEFAULT_DROP_PROJECT,
                        help=f'Drop project ({DEFAULT_DROP_PROJECT}).')

    args = parser.parse_args()
    delete(program=args.program,
           project=args.project,
           submission_client=submission_client(refresh_file=args.credentials_path, endpoint=args.endpoint),
           types=args.types,
           batch_size=args.batch_size,
           drop_project=args.drop_project
           )
