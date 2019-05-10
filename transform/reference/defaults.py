from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.ioutils import JSONEmitter
import os
import re

DEFAULT_OUTPUT_DIR = 'output/reference'
DEFAULT_EXPERIMENT_CODE = 'reference'


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




def path_to_type(path):
    """Get the type (snakecase) of a vertex file"""
    return snake_case(os.path.basename(path).split('.')[0])


def snake_case(name):
    """Converts name to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
