"""A transformer for gen3 project,reads cases bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter, obscure_dates
from gen3_etl.utils.schema import generate, template


def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    cases_emitter = emitter('case', output_dir=output_dir)
    bcc_cases_emitter = emitter('bcc_participant', output_dir=output_dir)
    cases = {}
    bcc_cases = {}
    submitter_ids = []
    for p in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            submitter_id = line.get('participantid', line.get('ParticipantID', None))
            submitter_ids.append(submitter_id)
            bcc_submitter_id = '{}-{}'.format(submitter_id, source)
            primary_site = line.get('site', None)
            case = {'type': 'case', 'experiments': {'submitter_id': experiment_code}, 'primary_site': primary_site, 'submitter_id': submitter_id, 'project_id': DEFAULT_PROJECT_ID}
            bcc_case = {'type': 'bcc_participant', 'case': {'submitter_id': submitter_id}, 'source': source, 'submitter_id': bcc_submitter_id, 'project_id': DEFAULT_PROJECT_ID}
            cases[submitter_id] = case
            if bcc_submitter_id in bcc_cases:
                # merge dupes
                bcc_case = bcc_cases[bcc_submitter_id]
            bcc_case.update(line)
            bcc_cases[bcc_submitter_id] = bcc_case
    for k in cases:
        cases_emitter.write(obscure_dates(cases[k], participantid=k, output_dir=output_dir))
    for k in bcc_cases:
        bcc_case = bcc_cases[k]
        for p in ['FirstName', 'MRN', 'LastName', 'DateOfBirth', '_labkeyurl_Gender_ID', '_labkeyurl_ParticipantID', 'Gender_ID']:
            del bcc_case[p]
        bcc_case = obscure_dates(bcc_case, output_dir=output_dir)
        bcc_cases_emitter.write(bcc_case)
    cases_emitter.close()
    bcc_cases_emitter.close()


if __name__ == "__main__":
    item_paths = ['source/bcc/admindemographics.json', 'source/bcc/vcancerparticipantoverview.json','source/bcc/vDemographicsForPlot.json']
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    p = os.path.join(args.output_dir, 'case.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)
    p = os.path.join(args.output_dir, 'bcc_participant.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)

    if args.schema:

        def my_callback(schema):
            # adds the source property
            schema['properties']['source'] = {'type': 'string'}
            schema['category'] = 'bcc extention'
            schema['properties']['case'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        item_paths = ['output/bcc/bcc_participant.json']
        link = {'name':'case', 'backref':'bcc_participants', 'label':'extends', 'target_type':'case',  'multiplicity': 'many_to_one', 'required': False }
        schema_path = generate(item_paths,'bcc_participant', output_dir='output/bcc', links=[link], callback=my_callback)
        assert os.path.isfile(p), 'should have an schema file {}'.format(schema_path)
        print(schema_path)
