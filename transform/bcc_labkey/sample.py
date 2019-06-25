"""A transformer for gen3 project,reads samples bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader
from gen3_etl.utils.cli import default_argument_parser

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, default_sample, default_case, emitter, missing_parent, save_missing_parents
from gen3_etl.utils.schema import generate, template


LOOKUP_PATHS = """
source/bcc/sample_type.json
""".strip().split()


def lookups():
    look_ups = {}
    for p in LOOKUP_PATHS:
        c = p.replace('source/bcc/','').replace('genetrails_','').replace('.json','')
        look_ups[c] = {}
        print(p, c)
        for line in reader(p):
            name = line['display_name']
            val = [line[k] for k in line if not k.startswith('_') and k.endswith('_id')][0]
            look_ups[c][val] = name
    return look_ups


LOOKUPS = lookups()


def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    cases = set([])
    for line in reader('{}/case.json'.format(output_dir)):
        cases.add(line['submitter_id'])

    diagnoses = set([])
    for line in reader('{}/diagnosis.json'.format(output_dir)):
        diagnoses.add(line['submitter_id'])

    missing_cases = set([])
    print('cases len {}'.format(len(cases)))

    # dedup
    samples = []
    samples_emitter = emitter('sample', output_dir=output_dir)
    bcc_samples_emitter = emitter('bcc_sample', output_dir=output_dir)
    missing_diagnoses = []
    for p in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            case_submitter_id = line.get('participantid', line.get('ParticipantID'))
            sample = default_sample(case_submitter_id, line=line, project_id=DEFAULT_PROJECT_ID)
            submitter_id = sample['submitter_id']
            if case_submitter_id not in cases:
                # print('no case {} for sample {} - skipping.'.format(case_submitter_id, submitter_id))
                missing_diagnoses.append(missing_parent(child_id=submitter_id, child_type='sample', parent_id=case_submitter_id, parent_type='case'))
                continue
            if submitter_id in samples:
                continue
            if sample['diagnoses']['submitter_id'] not in diagnoses:
                missing_diagnoses.append(missing_parent(child_id=submitter_id, child_type='sample', parent_id=sample['diagnoses']['submitter_id'], parent_type='diagnosis'))
                del sample['diagnoses']['submitter_id']

            bcc_submitter_id = '{}-{}'.format(submitter_id, source)
            samples_emitter.write(sample)
            samples.append(submitter_id)


            bcc_sample = {'type': 'bcc_sample', 'sample': {'submitter_id': submitter_id}, 'source': source, 'submitter_id': bcc_submitter_id, 'project_id': DEFAULT_PROJECT_ID}
            bcc_sample.update(line)
            if '_labkeyurl_sample_type_id' in bcc_sample:                
                bcc_sample['sample_type'] = LOOKUPS['sample_type'][bcc_sample['sample_type_id']]
                del bcc_sample['sample_type_id']
                del bcc_sample['_labkeyurl_sample_type_id']

            bcc_samples_emitter.write(bcc_sample)

            if case_submitter_id not in cases:
                missing_cases.add(case_submitter_id)
                cases.add(case_submitter_id)
    save_missing_parents(missing_diagnoses)



    # print('missing sample for {} cases'.format(len(missing_cases)))
    # samples_emitter = emitter('sample', output_dir=output_dir, append=True)
    # for participantid in cases:
    #     sample = default_sample(participantid)
    #     samples_emitter.write(sample)
    # samples_emitter.close()

    # samples_emitter.close()
    # bcc_samples_emitter.close()
    # print('missing cases for {} cases'.format(len(missing_cases)))
    # cases_emitter = emitter('case', output_dir=output_dir, append=True)
    # for participantid in missing_cases:
    #     case = default_case(DEFAULT_EXPERIMENT_CODE, participantid)
    #     cases_emitter.write(case)
    # cases_emitter.close()
    #
    # bcc_samples_emitter = emitter('bcc_sample', output_dir=output_dir, append=True)
    # for participantid in missing_cases:
    #     sample = default_sample(participantid)
    #     submitter_id = sample['submitter_id']
    #     bcc_sample = {'type': 'bcc_sample', 'sample': {'submitter_id': submitter_id}, 'source': source, 'submitter_id': bcc_submitter_id, 'project_id': DEFAULT_PROJECT_ID}
    #     bcc_samples_emitter.write(sample)
    # bcc_samples_emitter.close()


if __name__ == "__main__":
    item_paths = ['source/bcc/vbiolibraryspecimens.json', 'source/bcc/Samples.json', 'source/bcc/sample_genetrails_assay.json', 'source/bcc/sample.json']
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()
    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    if args.schema:

        def my_callback(schema):
            # adds the source property
            schema['properties']['source'] = {'type': 'string'}
            schema['properties']['sample_name'] = {'type': ['string', 'null']}
            schema['category'] = 'bcc extention'
            schema['properties']['sample'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        link = {'name':'sample', 'backref':'bcc_sample', 'label':'extends', 'target_type':'sample',  'multiplicity': 'many_to_one', 'required': False }
        schema_path = generate(item_paths,'bcc_sample', output_dir='output/bcc', links=[link], callback=my_callback)
        assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
        print(schema_path)
