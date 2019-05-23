"""A transformer for gen3 project,reads aliquots bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader
from gen3_etl.utils.cli import default_argument_parser

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, default_aliquot, default_sample, default_diagnosis, emitter
from gen3_etl.utils.schema import generate, template


def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    aliquots_emitter = emitter('aliquot', output_dir=output_dir)
    for line in reader('{}/sample.json'.format(output_dir)):
        assert 'submitter_id' in line, line
        aliquots_emitter.write(default_aliquot(line['submitter_id'], project_id=DEFAULT_PROJECT_ID))
    aliquots_emitter.close()


if __name__ == "__main__":
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()
    transform(item_paths=[], output_dir=args.output_dir, experiment_code=args.experiment_code)
