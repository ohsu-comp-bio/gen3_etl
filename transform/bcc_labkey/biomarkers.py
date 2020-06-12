"""A transformer for gen3 project,reads treatments bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter, default_treatment, missing_parent, save_missing_parents, obscure_dates
from gen3_etl.utils.schema import generate, template

def transform_gen3(item_paths, output_dir, project_id, compresslevel=0):
    """Creates gen3.treatment, returns set of treatment_ids."""
    case_lookup = {line['MRN']: line['OPTR'] for line in reader('{}/bcc-cases.tsv'.format('source/bcc'))}

    biomarker_emitter = emitter('bcc_biomarker', output_dir=output_dir)
    for item_path in item_paths:
        biomarkers = [line for line in reader(item_path)]
        # missing_cases = [b['MRN'] for b in biomarkers if b['MRN'] not in case_lookup]
        def add_case(b):
            case_submitter_id = case_lookup[b['MRN']]
            submitter_id = '{}-{}-bcc_biomarker'.format(case_submitter_id, b['ID_Event'])
            for p in ["MRN", "Participant ID", "_not_available_notes", "_not_available_reason_id", "cBiomarker Label dont use",]:
                del b[p]
            for p in ["CA19 Values After Specimen Collection", "Order Proc ID", "assay version id", "biomarker level", "unit of measure id", ]:
                new_p = p.replace(' ','_').lower()
                b[new_p] = b[p]
                del b[p]
            b['cbiomarker_label'] = b["cBiomarker Label use this"]
            del b["cBiomarker Label use this"]
            biomarker = {'type': 'bcc_biomarker', 'cases': {'submitter_id': case_submitter_id }, 'submitter_id': submitter_id,
                         'project_id': project_id}

            biomarker.update(b)
            return biomarker
        biomarkers_with_case = [add_case(b) for b in biomarkers if b['MRN'] in case_lookup]
        print('there are',len(biomarkers_with_case), 'biomarkers with cases, out of ', len(biomarkers), 'biomarkers')
        [biomarker_emitter.write(obscure_dates(b)) for b in biomarkers_with_case]
    biomarker_emitter.close()


if __name__ == "__main__":
    item_paths = ['source/bcc/biomarkers.tsv', ]
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()
    transform_gen3(item_paths, args.output_dir, args.project_id)

    if args.schema:

        def my_callback(schema):
            schema['category'] = 'bcc extention'
            schema['properties']['case'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        item_paths = ['output/bcc/bcc_biomarker.json', ]

        link = {'name':'cases', 'backref':'bcc_biomarkers', 'label':'extends', 'target_type':'case',  'multiplicity': 'many_to_one', 'required': False }
        schema_path = generate(item_paths,'bcc_biomarker', output_dir='output/bcc', links=[link], callback=my_callback)
        assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
        print(schema_path)


# [
#   "CA19 Values After Specimen Collection",
#   "Date",
#   "ID_Event",
#   "Order Proc ID",
#   "Participant ID",
#   "_not_available_notes",
#   "_not_available_reason_id",
#   "assay version id",
#   "biomarker level",
#   "cBiomarker Label dont use",
#   "cBiomarker Label use this",
#   "case_submitter_id",
#   "unit of measure id",
#   "z_CreatedTS",
#   "z_Creator",
#   "z_ModifiedTS",
#   "z_Modifier"
# ]
