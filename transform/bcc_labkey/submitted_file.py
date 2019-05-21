
"""A transformer for gen3 project,reads treatments bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter, default_treatment, missing_parent, save_missing_parents
from gen3_etl.utils.schema import generate, template

def transform_gen3(item_paths, output_dir, project_id, compresslevel=0):
    """Creates gen3.treatment, returns set of treatment_ids."""
    cases = set([line['submitter_id'] for line in reader('{}/case.json'.format(output_dir))])

    submitted_file_emitter = emitter('submitted_file', output_dir=output_dir)
    for item_path in item_paths:
        submitted_files = [line for line in reader(item_path)]
        # missing_cases = [b['MRN'] for b in submitted_files if b['MRN'] not in case_lookup]
        def add_case(b):
            case_submitter_id = case_lookup[b['MRN']]
            submitter_id = '{}-{}-bcc_submitted_file'.format(case_submitter_id, b['ID_Event'])
            for p in ["MRN", "Participant ID", "_not_available_notes", "_not_available_reason_id", "cBiomarker Label dont use",]:
                del b[p]
            for p in ["CA19 Values After Specimen Collection", "Order Proc ID", "assay version id", "submitted_file level", "unit of measure id", ]:
                new_p = p.replace(' ','_').lower()
                b[new_p] = b[p]
                del b[p]
            b['csubmitted_file_label'] = b["cBiomarker Label use this"]
            del b["cBiomarker Label use this"]
            submitted_file = {'type': 'bcc_submitted_file', 'cases': {'submitter_id': case_submitter_id }, 'submitter_id': submitter_id,
                         'project_id': project_id}

            submitted_file.update(b)
            return submitted_file
        submitted_files_with_case = [add_case(b) for b in submitted_files if b['MRN'] in case_lookup]
        print('there are',len(submitted_files_with_case), 'submitted_files with cases, out of ', len(submitted_files), 'submitted_files')
        [submitted_file_emitter.write(b) for b in submitted_files_with_case]
    submitted_file_emitter.close()


if __name__ == "__main__":
    item_paths = ['source/bcc/documentstore.json', ]
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()
    transform_gen3(item_paths, args.output_dir, args.project_id)

    if args.schema:

        def my_callback(schema):
            schema['category'] = 'bcc extention'
            schema['properties']['case'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        item_paths = ['output/bcc/submitted_file.json', ]

        link = {'name':'cases', 'backref':'bcc_submitted_files', 'label':'extends', 'target_type':'case',  'multiplicity': 'many_to_one', 'required': False }
        schema_path = generate(item_paths,'bcc_submitted_file', output_dir='output/bcc', links=[link], callback=my_callback)
        assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
        print(schema_path)
