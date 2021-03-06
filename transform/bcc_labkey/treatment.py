"""A transformer for gen3 project,reads treatments bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter, default_treatment, missing_parent, save_missing_parents, obscure_dates
from gen3_etl.utils.schema import generate, template

LOOKUP_PATHS = """
source/bcc/treatment_agent.json
source/bcc/treatment_agent_alt_name.json
source/bcc/treatment_chemotherapy_regimen.json
source/bcc/unit_of_measure.json
source/bcc/treatment_combo.json
source/bcc/treatment_combo_agents.json
source/bcc/treatment_type.json
source/bcc/delivery_method.json
""".strip().split()


def get_uniq(line):
    """Returns a uniq value other than treatment type or submitter id."""
    return line.get('lsid', line.get('date', line.get('definitive_resection_date', None)))

def transform_gen3(item_paths, output_dir, project_id, compresslevel=0):
    """Creates gen3.treatment, returns set of treatment_ids."""
    diagnoses = set([line['submitter_id'] for line in reader('{}/diagnosis.json'.format(output_dir))])
    treatment_emitter = emitter('treatment', output_dir=output_dir)
    treatment_ids = set([])
    missing_diagnoses = []
    for p,treatment_type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            participantid = line.get('ParticipantID', line.get('participantid', None))
            assert participantid, 'ParticipantID not in {} {}'.format(p, line.keys())
            diagnosis_submitter_id = '{}-diagnosis'.format(participantid)
            treatment_submitter_id = '{}-{}-{}'.format(diagnosis_submitter_id, treatment_type, get_uniq(line))
            if diagnosis_submitter_id not in diagnoses:
                missing_diagnoses.append(missing_parent(parent_id=diagnosis_submitter_id, parent_type='diagnosis', child_id=treatment_submitter_id, child_type='treatment'))
                print('skipping missing diagnosis', treatment_submitter_id)
                continue
            if treatment_submitter_id in treatment_ids:
                print('skipping ',treatment_submitter_id, p, line.keys())
                continue
            treatment_ids.add(treatment_submitter_id)
            treatment = default_treatment(treatment_submitter_id, diagnosis_submitter_id, treatment_type, project_id)
            treatment = obscure_dates(treatment, output_dir=output_dir, participantid=participantid)
            treatment_emitter.write(treatment)
    save_missing_parents(missing_diagnoses)
    return treatment_ids


def transform_chemotherapy(item_paths, output_dir, project_id, treatment_ids, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    bcc_treatment_emitter = emitter('bcc_chemotherapy', output_dir=output_dir)
    for p,type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            line['source'] = source
            if callback:
                line = callback(line)
            diagnosis_submitter_id = '{}-diagnosis'.format(line['ParticipantID'])
            treatment_submitter_id = '{}-Chemotherapy-{}'.format(diagnosis_submitter_id, get_uniq(line))
            if treatment_submitter_id not in treatment_ids:
                # print('transform_chemotherapy {} not in treatment_ids, skipping.'.format(treatment_submitter_id))
                continue
            bcc_treatment = {
                'type': 'bcc_chemotherapy',
                'project_id': project_id,
                'treatment': {'submitter_id': treatment_submitter_id},
                'submitter_id': '{}-{}-{}'.format(treatment_submitter_id, line['days'], line.get('treatment_description', line.get('treatment_agent', 'na')))
                }
            bcc_treatment.update(line)
            bcc_treatment = obscure_dates(bcc_treatment, output_dir=output_dir)
            bcc_treatment_emitter.write(bcc_treatment)
    bcc_treatment_emitter.close()


def transform_surgery(item_paths, output_dir, project_id, treatment_ids, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    bcc_treatment_emitter = emitter('bcc_surgery', output_dir=output_dir)
    bcc_treatment_submitter_ids = []
    for p,type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            line['source'] = source
            if callback:
                line = callback(line)
            participantid = line.get('ParticipantID', line.get('participantid', None))
            assert participantid, 'ParticipantID not in {} {}'.format(p, line.keys())
            diagnosis_submitter_id = '{}-diagnosis'.format(participantid)
            treatment_submitter_id = '{}-Surgery-{}'.format(diagnosis_submitter_id, get_uniq(line))
            bcc_treatment_submitter_id = '{}-bcc_surgery'.format(treatment_submitter_id)
            if treatment_submitter_id not in treatment_ids:
                # print('transform_surgery {} not in treatment_ids, skipping.'.format(treatment_submitter_id))
                continue
            if bcc_treatment_submitter_id in bcc_treatment_submitter_ids:
                # print('transform_surgery {} in bcc_treatment_submitter_ids, skipping.'.format(treatment_submitter_id))
                continue
            bcc_treatment_submitter_ids.append(bcc_treatment_submitter_id)
            bcc_treatment = {
                'type': 'bcc_surgery',
                'project_id': project_id,
                'treatment': {'submitter_id': treatment_submitter_id},
                'submitter_id': bcc_treatment_submitter_id
                }
            if 'type' in line and p == 'source/bcc/vResectionDate.json':
                del line['type']
            bcc_treatment.update(line)
            bcc_treatment = obscure_dates(bcc_treatment, output_dir=output_dir)
            bcc_treatment_emitter.write(bcc_treatment)
    bcc_treatment_emitter.close()


def transform_radiotherapy(item_paths, output_dir, project_id, treatment_ids, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    bcc_treatment_emitter = emitter('bcc_radiotherapy', output_dir=output_dir)
    bcc_treatment_submitter_ids = []
    for p,type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            line['source'] = source
            if callback:
                line = callback(line)
            participantid = line.get('ParticipantID', line.get('participantid', None))
            assert participantid, 'ParticipantID not in {} {}'.format(p, line.keys())
            diagnosis_submitter_id = '{}-diagnosis'.format(participantid)
            treatment_submitter_id = '{}-Radiotherapy-{}'.format(diagnosis_submitter_id, get_uniq(line))
            bcc_treatment_submitter_id = '{}-bcc_radiotherapy'.format(treatment_submitter_id)
            if treatment_submitter_id not in treatment_ids:
                print('transform_radiotherapy {} not in treatment_ids, skipping.'.format(treatment_submitter_id))
                continue
            if bcc_treatment_submitter_id in bcc_treatment_submitter_ids:
                print('transform_radiotherapy {} in bcc_treatment_submitter_ids, skipping.'.format(treatment_submitter_id))
                continue
            bcc_treatment_submitter_ids.append(bcc_treatment_submitter_id)
            bcc_treatment = {
                'type': 'bcc_radiotherapy',
                'project_id': project_id,
                'treatment': {'submitter_id': treatment_submitter_id},
                'submitter_id': bcc_treatment_submitter_id
                }
            line['radiotherapy_type'] = line['type']
            del line['type']
            bcc_treatment.update(line)
            bcc_treatment = obscure_dates(bcc_treatment, output_dir=output_dir)
            bcc_treatment_emitter.write(bcc_treatment)
    bcc_treatment_emitter.close()


def lookups():
    look_ups = {}
    for p in LOOKUP_PATHS:
        c = p.replace('source/bcc/','').replace('.json','')
        look_ups[c] = {}
        print(p, c)
        for line in reader(p):
            name = line.get('display_name', line.get('alt_display_name', None))
            val = [line[k] for k in line if not k.startswith('_') and k.endswith('_id')][0]
            look_ups[c][val] = name
    return look_ups


LOOKUPS = lookups()


def my_callback(line):
    """Remove fields that start with _, fix key names with embedded /, fix id lookups """
    for k in [k for k in line if k.startswith('_')]:
        del line[k]

    for k in [k for k in line if '/' in k]:
        line[k.split('/')[1]] = line[k]
        del line[k]

    for k in [k for k in line if k.endswith('_id')]:
        lup = k.replace('_id', '')
        if line[k]:
            try:
                line[lup] = LOOKUPS[lup][line[k]]
            except Exception as e:
                print(lup, k, line[k])
                print('******')
                print(LOOKUPS[lup])
                print('******')
                raise e
        del line[k]
    if 'chromosome' in line:
        line['chromosome'] = str(line['chromosome'].replace('chr',''))
    if 'gene' in line:
        line['gene_symbol'] = line['gene']
        del line['gene']

    line = obscure_dates(line)
    return line


def my_schema_callback(schema):
    """Remove fields that start with _, fix key names with embedded /, fix id lookups """
    for k in [k for k in schema['properties'] if k.startswith('_')]:
        del schema['properties'][k]
    for k in [k for k in schema['properties'] if '/' in k]:
        schema['properties'][k.split('/')[1]] = schema['properties'][k]
        del schema['properties'][k]
    for k in [k for k in schema['properties'] if k.endswith('_id')]:
        if k in schema['required'] or k in schema['systemProperties']:
            continue
        schema['properties'][k.replace('_id', '')] = {'type': ['string', 'null']}  # schema['properties'][k]
        del schema['properties'][k]
    # adds extra properties not found in
    schema['category'] = 'bcc extention'
    schema['properties']['treatment'] = {'$ref': '_definitions.yaml#/to_one'}
    return schema

    return schema


if __name__ == "__main__":
    item_paths = ['source/bcc/treatment_chemotherapy_ohsu.json','source/bcc/treatment_chemotherapy_manually_entered.json']
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    item_paths = [
        ('source/bcc/treatment_chemotherapy_ohsu.json', 'Chemotherapy', my_callback),
        ('source/bcc/treatment_chemotherapy_manually_entered.json', 'Chemotherapy', my_callback),
        # ('source/bcc/vResectionDate.json', 'Surgery', None),
        ('source/bcc/voncologsurgery.json', 'Surgery', None),
        ('source/bcc/Radiotherapy.json', 'Radiotherapy', None)
    ]
    treatment_ids = transform_gen3(item_paths, output_dir=args.output_dir, project_id=args.project_id)
    # print('\n'.join(treatment_ids))

    item_paths = [
        ('source/bcc/treatment_chemotherapy_ohsu.json', 'Chemotherapy', my_callback),
        ('source/bcc/treatment_chemotherapy_manually_entered.json', 'Chemotherapy', my_callback),
    ]
    transform_chemotherapy(item_paths, treatment_ids=treatment_ids, output_dir=args.output_dir, project_id=args.project_id)
    item_paths = [
        'output/bcc/bcc_chemotherapy.json',
    ]
    link = {'name':'treatment', 'backref':'bcc_chemotherapy', 'label':'describes', 'target_type':'treatment',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_chemotherapy', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)

    item_paths = [
        ('source/bcc/vResectionDate.json', 'Surgery', my_callback),
        ('source/bcc/voncologsurgery.json', 'Surgery', my_callback),
    ]
    transform_surgery(item_paths, treatment_ids=treatment_ids, output_dir=args.output_dir, project_id=args.project_id)
    item_paths = [
        'output/bcc/bcc_surgery.json',
    ]
    link = {'name':'treatment', 'backref':'bcc_surgery', 'label':'describes', 'target_type':'treatment',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_surgery', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)

    item_paths = [
        ('source/bcc/Radiotherapy.json', 'Radiotherapy', my_callback),
    ]
    transform_radiotherapy(item_paths, treatment_ids=treatment_ids, output_dir=args.output_dir, project_id=args.project_id)
    item_paths = [
        'output/bcc/bcc_radiotherapy.json',
    ]
    link = {'name':'treatment', 'backref':'bcc_radiotherapy', 'label':'describes', 'target_type':'treatment',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_radiotherapy', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)
