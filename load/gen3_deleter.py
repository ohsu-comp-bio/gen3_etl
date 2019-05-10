"""Utility, deletes projects and nodes.  Edit types array for your deployment."""
import os
import sys

from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.gen3 import delete_all, submission_client

DEFAULT_PROGRAM = None
DEFAULT_PROJECT = None
DEFAULT_CREDENTIALS_PATH = os.path.join('config', 'credentials.json')
DEFAULT_ENDPOINT = 'https://localhost'


def delete(program, project, submission_client):
    """Delete all content from project."""

    delete_all(submission_client, program, project, types=[
        'submitted_methylation',
        'submitted_somatic_mutation',
        'read_group',
        'demographic',
        'aliquot',
        'sample',
        'bcc_diagnosis',
        'diagnosis',
        'bcc_demographic',
        'demographic',
        'bcc_participant',
        'case',
        'experiment'])
    try:
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

    args = parser.parse_args()
    delete(program=args.program,
           project=args.project,
           submission_client=submission_client(refresh_file=args.credentials_path,
                                               endpoint=args.endpoint))
