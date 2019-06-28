"""A transformer for gen3 project,reads diagnosiss bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, default_diagnosis, default_case, emitter,  missing_parent, save_missing_parents
from gen3_etl.utils.schema import generate, template



def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Reads bcc labkey json and writes participantid, dob json."""
    dob_emitter = emitter('bcc_participant_dob', output_dir=output_dir)

    for p in item_paths:
        for line in reader(p):
            dob_emitter.write({'participantid': line['ParticipantID'], 'DateOfBirth': line['DateOfBirth']})
    dob_emitter.close()        

if __name__ == "__main__":
    transform(['source/bcc/admindemographics.json'] , DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE)
