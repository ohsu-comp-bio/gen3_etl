from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.ioutils import JSONEmitter
from gen3_etl.utils.models import default_case, default_sample, default_aliquot, default_diagnosis, default_treatment, default_observation

import os
import hashlib
import json

DEFAULT_OUTPUT_DIR = 'output/bcc'
DEFAULT_EXPERIMENT_CODE = 'bcc'
DEFAULT_PROJECT_ID = 'ohsu-bcc'

def emitter(type=None, output_dir=DEFAULT_OUTPUT_DIR, **kwargs):
    """Creates a default emitter for type."""
    return JSONEmitter(os.path.join(output_dir, '{}.json'.format(type)), compresslevel=0, **kwargs)


def default_parser(output_dir, experiment_code, project_id):
    parser = default_argument_parser(
        output_dir=output_dir,
        description='Reads bcc json and writes gen3 json ({}).'.format(output_dir)
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


def missing_parent(**kwargs):
    """Returns dict of missing item."""
    for p in ['parent_id', 'parent_type', 'child_id', 'child_type']:
        assert p in kwargs, 'missing {}'.format(p)
    return kwargs

def save_missing_parents(missing_parents, output_dir=DEFAULT_OUTPUT_DIR):
    """Saves list of missing item. Dedupes."""
    with open('{}/missing.json'.format(output_dir), 'a') as missing_parent_file:
        dedupe = set([])
        for mp in missing_parents:
            js = json.dumps(mp, separators=(',',':'))
            check_sum = hashlib.sha256(js.encode('utf-8')).digest()
            if check_sum in dedupe:
                continue
            dedupe.add(check_sum)
            missing_parent_file.write(js)
            missing_parent_file.write('\n')
