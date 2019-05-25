"""A transformer for gen3 project,reads cases bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json
import csv

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter, DEFAULT_INPUT_FILE, exclude_row
from gen3_etl.utils.schema import generate, template


def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read medable csv and writes gen3 json."""
    cases_emitter = emitter('case', output_dir=output_dir)
    cases = set([])
    with open(item_paths[0], newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if exclude_row(row):
                continue
            submitter_id = row['c_public_user._id']
            if len(submitter_id) == 0:
                continue
            cases.add(submitter_id)

    for submitter_id in cases:
        case = {'type': 'case', 'experiments': {'submitter_id': experiment_code}, 'submitter_id': submitter_id, 'project_id': DEFAULT_PROJECT_ID}
        cases_emitter.write(case)
    cases_emitter.close()


if __name__ == "__main__":
    item_paths = [DEFAULT_INPUT_FILE]
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    p = os.path.join(args.output_dir, 'case.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)
    #
    # if args.schema:
    #
    #     def my_callback(schema):
    #         # adds the source property
    #         schema['properties']['source'] = {'type': 'string'}
    #         schema['category'] = 'bcc extention'
    #         schema['properties']['case'] = {'$ref': '_definitions.yaml#/to_one'}
    #         return schema
    #
    #     link = {'name':'case', 'backref':'bcc_participants', 'label':'extends', 'target_type':'case',  'multiplicity': 'many_to_one', 'required': False }
    #     schema_path = generate(item_paths,'bcc_participant', output_dir='output/bcc', links=[link], callback=my_callback)
    #     assert os.path.isfile(p), 'should have an schema file {}'.format(schema_path)
    #     print(schema_path)
