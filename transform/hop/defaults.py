from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.ioutils import JSONEmitter
from gen3_etl.utils.models import default_case, default_sample, default_aliquot, default_diagnosis, default_treatment, default_observation

import os
import hashlib
import json

DEFAULT_OUTPUT_DIR = 'output/hop'
DEFAULT_EXPERIMENT_CODE = 'hop'
DEFAULT_PROJECT_ID = 'ohsu-hop'

DEFAULT_INPUT_FILE = 'source/hop/healthy_oregon_project_hop_responses.csv'


def exclude_row(row):
    """Filters bad rows."""
    if row['c_step.object'] != 'c_step':
        return True
    return False


def emitter(type=None, output_dir=DEFAULT_OUTPUT_DIR, **kwargs):
    """Creates a default emitter for type."""
    return JSONEmitter(os.path.join(output_dir, '{}.json'.format(type)), compresslevel=0, **kwargs)


def default_parser(output_dir, experiment_code, project_id):
    parser = default_argument_parser(
        output_dir=output_dir,
        description='Reads hop json and writes gen3 json ({}).'.format(output_dir)
    )
    parser.add_argument('--experiment_code', type=str,
                        default=experiment_code,
                        help='Name of gen3 experiment ({}).'.format(experiment_code))
    parser.add_argument('--project_id', type=str,
                        default=project_id,
                        help='Name of gen3 program-project ({}).'.format(project_id))
    parser.add_argument('--schema', type=bool,
                        default=True,
                        help='generate schemas (true).'.format(experiment_code))
    return parser
