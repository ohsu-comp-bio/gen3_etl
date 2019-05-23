from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.ioutils import JSONEmitter
from gen3_etl.utils.models import default_case, default_sample, default_aliquot, default_diagnosis

import os

DEFAULT_OUTPUT_DIR = 'output/bcc'
DEFAULT_EXPERIMENT_CODE = 'bcc'


def emitter(type=None, output_dir=DEFAULT_OUTPUT_DIR, **kwargs):
    """Creates a default emitter for type."""
    return JSONEmitter(os.path.join(output_dir, '{}.json'.format(type)), compresslevel=0, **kwargs)


def default_parser():
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads bcc json and writes gen3 json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )
    parser.add_argument('--experiment_code', type=str,
                        default=DEFAULT_EXPERIMENT_CODE,
                        help='Name of existing gen3 experiment ({}).'.format(DEFAULT_EXPERIMENT_CODE))
    parser.add_argument('--schema', type=bool,
                        default=True,
                        help='generate schemas (true).'.format(DEFAULT_EXPERIMENT_CODE))
    return parser
