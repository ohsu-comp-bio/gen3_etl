"""A transformer for gen3 project,reads diagnosiss bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, default_diagnosis, default_case, emitter,  missing_parent, save_missing_parents
from gen3_etl.utils.schema import generate, template


def t(output_dir):
    """Read bcc labkey json and writes gen3 json."""
    cases = set([])
    for line in reader('{}/bcc_participant.json'.format(output_dir)):
        cases.add(line['submitter_id'])
sss


if __name__ == "__main__":
    t('output/bcc')
