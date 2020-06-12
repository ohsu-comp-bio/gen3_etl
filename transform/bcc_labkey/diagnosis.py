"""A transformer for gen3 project,reads diagnosiss bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, default_diagnosis, default_case, emitter,  missing_parent, save_missing_parents, obscure_dates
from gen3_etl.utils.schema import generate, template

# """
# {
#   "participantid": "2001",
#   "site": "Pancreas",
#   "lymphvascular_invasion": null,
#   "stage": "2B - T1-3, N1, M0",
#   "definitive_grade_at_dx": "Grade III Poorly Differentiated",
#   "diagnosis_date": "2007/01/09 00:00:00",
#   "diagnosis": "Infiltrating duct carcinoma, NOS (C50._)",
#   "tumor_size_at_first_tx": 75,
#   "diagnosis_category": "Adenocarcinoma"
# }
# """

def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    cases = set([])
    for line in reader('{}/case.json'.format(output_dir)):
        cases.add(line['submitter_id'])
    diagnoses_emitter = emitter('diagnosis', output_dir=output_dir)
    bcc_diagnosis_emitter = emitter('bcc_diagnosis', output_dir=output_dir)
    diagnosises = {}
    bcc_diagnosises = {}
    missing_cases = set([])
    for p in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            case_submitter_id = line['participantid']
            bcc_submitter_id = '{}-{}'.format(case_submitter_id, source)
            diagnosis = default_diagnosis(case_submitter_id, project_id=DEFAULT_PROJECT_ID, line=line)
            submitter_id = diagnosis['submitter_id']
            bcc_diagnosis = {'type': 'bcc_diagnosis', 'diagnosis': {'submitter_id': submitter_id}, 'source': source, 'submitter_id': bcc_submitter_id, 'project_id': DEFAULT_PROJECT_ID}

            if bcc_submitter_id in bcc_diagnosises:
                bcc_diagnosis = bcc_diagnosises[bcc_submitter_id]

            # we will use the name 'diagnosis' as a link back to gen3.diagnosis
            line['diagnosis_name'] = line.get('diagnosis', None)
            del line['diagnosis']
            bcc_diagnosis.update(line)


            diagnosises[submitter_id] = diagnosis
            bcc_diagnosises[bcc_submitter_id] = bcc_diagnosis
            if case_submitter_id not in cases:
                print('no case for: >{}<'.format(case_submitter_id))
                missing_cases.add(case_submitter_id)

    for k in diagnosises:
        diagnosises[k] = obscure_dates(diagnosises[k], output_dir=output_dir, participantid=diagnosises[k]['cases']['submitter_id'])
        diagnoses_emitter.write(diagnosises[k])

    cases = missing_cases - cases
    print('missing diagnosis for {} cases'.format(len(cases)))
    for participantid in cases:
        diagnosis = default_diagnosis(participantid, project_id=DEFAULT_PROJECT_ID)
        diagnosis = obscure_dates(diagnosis, output_dir=output_dir)
        diagnoses_emitter.write(diagnosis)
    diagnoses_emitter.close()

    print('missing cases for {} cases'.format(len(missing_cases)))
    cases_emitter = emitter('case', output_dir=output_dir, append=True)
    for participantid in missing_cases:
        case = default_case(DEFAULT_EXPERIMENT_CODE, participantid, DEFAULT_PROJECT_ID)
        case = obscure_dates(case, output_dir=output_dir)
        cases_emitter.write(case)
    cases_emitter.close()

    bcc_diagnosises_emitter = emitter('bcc_diagnosis', output_dir=output_dir)
    for k in bcc_diagnosises:
        bcc_diagnosises[k] = obscure_dates(bcc_diagnosises[k], output_dir=output_dir)
        bcc_diagnosises_emitter.write(bcc_diagnosises[k])
    bcc_diagnosises_emitter.close()



if __name__ == "__main__":
    item_paths = ['source/bcc/voncologdiagnosis.json']
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()
    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    p = os.path.join(args.output_dir, 'diagnosis.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)
    p = os.path.join(args.output_dir, 'bcc_diagnosis.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)

    if args.schema:

        def my_callback(schema):
            # adds the source property
            schema['properties']['source'] = {'type': 'string'}
            schema['properties']['diagnosis_name'] = {'type': ['string', 'null']}
            schema['category'] = 'bcc extention'
            schema['properties']['diagnosis'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        link = {'name':'diagnosis', 'backref':'bcc_diagnosis', 'label':'extends', 'target_type':'diagnosis',  'multiplicity': 'many_to_one', 'required': False }
        schema_path = generate(item_paths,'bcc_diagnosis', output_dir='output/bcc', links=[link], callback=my_callback)
        assert os.path.isfile(p), 'should have an schema file {}'.format(schema_path)
        print(schema_path)
