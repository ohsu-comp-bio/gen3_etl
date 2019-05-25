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
    survey_emitter = emitter('survey', output_dir=output_dir)

    def answer(row):
        """Deduces the answer."""
        fields = ['c_value', 'c_value[0]','c_value[1]', 'c_value[2]','c_value[3]','c_value[4]']
        for f in fields:
            if row[f]:
                return row[f]

    with open(item_paths[0], newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if exclude_row(row):
                continue            
            case_submitter_id = row['c_public_user._id']
            if len(case_submitter_id) == 0:
                continue
            submitter_id = row['_id']
            survey = {
                'type': 'hop_survey',
                'cases': {'submitter_id': case_submitter_id },
                'submitter_id': submitter_id,
                'project_id': DEFAULT_PROJECT_ID,
                'name': row['c_step.c_name'],  # row['c_step._id'],
                'answer': answer(row),
            }
            survey_emitter.write(survey)
    survey_emitter.close()


if __name__ == "__main__":
    item_paths = [DEFAULT_INPUT_FILE]
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    p = os.path.join(args.output_dir, 'survey.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)

    if args.schema:

        def my_callback(schema):
            # adds the source property
            schema['category'] = 'hop extention'
            schema['properties']['cases'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        item_paths = ['output/hop/survey.json']

        link = {'name':'cases', 'backref':'hop_survey', 'label':'extends', 'target_type':'case',  'multiplicity': 'one_to_one', 'required': False }
        schema_path = generate(item_paths,'hop_survey', output_dir=args.output_dir, links=[link], callback=my_callback)
        assert os.path.isfile(p), 'should have an schema file {}'.format(schema_path)
        print(schema_path)
