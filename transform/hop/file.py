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
    file_emitter = emitter('submitted_file', output_dir=output_dir)

    with open(item_paths[0], newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if exclude_row(row):
                continue            
            case_submitter_id = row['c_public_user._id']
            if len(case_submitter_id) == 0:
                continue
            if len(row['c_file.ETag']) == 0:
                continue

            submitter_id = '{}-sf'.format(row['_id'])

            # {
            #   "*data_type": null,
            #   "urls": null,
            #   "*data_format": null,
            #   "type": "submitted_file",
            #   "object_id": null,
            #   "*submitter_id": null,
            #   "*data_category": null,
            #   "*md5sum": null,
            #   "*file_size": null,
            #   "aliquots": {
            #     "submitter_id": null
            #   },
            #   "*file_name": null,
            #   "cases": {
            #     "submitter_id": null
            #   },
            #   "project_id": null,
            #   "state_comment": null,
            #   "projects": {
            #     "code": null
            #   }
            # }
            file = {
                'type': 'submitted_file',
                'cases': {'submitter_id': case_submitter_id },
                'submitter_id': submitter_id,
                'project_id': DEFAULT_PROJECT_ID,
                'data_type': row['c_file.mime'],
                'md5sum':  row['c_file.ETag'],
                'file_size':  row['c_file.size'],
                'file_name':  row['c_file.path'],
            }
            file_emitter.write(file)
    file_emitter.close()


if __name__ == "__main__":
    item_paths = [DEFAULT_INPUT_FILE]
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    p = os.path.join(args.output_dir, 'submitted_file.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)

    if args.schema:

        def my_callback(schema):
            # adds the source property
            schema['category'] = 'hop extention'
            schema['properties']['cases'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        item_paths = ['output/hop/file.json']

        link = {'name':'cases', 'backref':'submitted_file', 'label':'extends', 'target_type':'case',  'multiplicity': 'one_to_one', 'required': False }
        schema_path = generate(item_paths,'submitted_file', output_dir=args.output_dir, links=[link], callback=my_callback)
        assert os.path.isfile(p), 'should have an schema file {}'.format(schema_path)
        print(schema_path)
