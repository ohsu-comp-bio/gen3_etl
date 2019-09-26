"""A transformer for gen3 project,reads lesions bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter, default_observation, missing_parent, save_missing_parents, obscure_dates
from gen3_etl.utils.schema import generate, template

LOOKUP_PATHS = """
source/bcc/lesion_procedure.json
source/bcc/lesion_site.json
source/bcc/lesion_type.json
source/bcc/unit_of_measure.json
source/bcc/assay_categories.json
source/bcc/assay_version.json
""".strip().split()


def transform_gen3(item_paths, output_dir, project_id, compresslevel=0):
    """Creates gen3.lesion, returns set of lesion_ids."""
    cases = set([line['submitter_id'] for line in reader('{}/case.json'.format(output_dir))])
    observation_emitter = emitter('observation', output_dir=output_dir)
    observation_ids = set([])
    missing_cases = []
    for p,observation_type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            participantid = line.get('ParticipantID', line.get('participantid', None))
            assert participantid, 'ParticipantID not in {} {}'.format(p, line.keys())
            case_submitter_id = participantid
            observation = default_observation(case_submitter_id, project_id, line['date'], observation_type, line)
            observation_submitter_id = observation['submitter_id']
            if case_submitter_id not in cases:
                missing_cases.append(missing_parent(parent_id=case_submitter_id, parent_type='case', child_id=observation_submitter_id, child_type='observation'))
                continue
            if observation_submitter_id in observation_ids:
                continue
            observation_ids.add(observation_submitter_id)
            observation = obscure_dates(observation, output_dir=output_dir, participantid=observation['cases']['submitter_id'])
            observation_emitter.write(observation)
    save_missing_parents(missing_cases)
    return observation_ids


def transform_lesion(item_paths, output_dir, project_id, observation_ids, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    bcc_lesion_emitter = emitter('bcc_lesion', output_dir=output_dir)
    for p,observation_type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            participantid = line.get('ParticipantID', line.get('participantid', None))
            observation = default_observation(participantid, project_id, line['date'], observation_type, line)
            observation_submitter_id = observation['submitter_id']
            lesion_submitter_id = '{}-bcc_lesion'.format(observation_submitter_id)
            if observation_submitter_id not in observation_ids:
                print('transform_lesion {} not in observation_ids, skipping.'.format(observation_submitter_id))
                continue
            bcc_lesion = {
                'type': 'bcc_lesion',
                'project_id': project_id,
                'observation': {'submitter_id': observation_submitter_id},
                'submitter_id': lesion_submitter_id
                }
            line['source'] = source
            if callback:
                line = callback(line)
            bcc_lesion.update(line)
            bcc_lesion = obscure_dates(bcc_lesion, output_dir=output_dir)
            bcc_lesion_emitter.write(bcc_lesion)
    bcc_lesion_emitter.close()


def transform_weight(item_paths, output_dir, project_id, observation_ids, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    bcc_weight_emitter = emitter('bcc_weight', output_dir=output_dir)
    for p,observation_type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            participantid = line.get('ParticipantID', line.get('participantid', None))
            observation = default_observation(participantid, project_id, line['date'], observation_type, line)
            observation_submitter_id = observation['submitter_id']
            weight_submitter_id = '{}-bcc_weight'.format(observation_submitter_id)
            if observation_submitter_id not in observation_ids:
                print('transform_weight {} not in observation_ids, skipping.'.format(weight_submitter_id))
                continue
            bcc_lesion = {
                'type': 'bcc_weight',
                'project_id': project_id,
                'observation': {'submitter_id': observation_submitter_id},
                'submitter_id': weight_submitter_id
                }
            line['source'] = source
            if callback:
                line = callback(line)
            bcc_lesion.update(line)
            bcc_lesion = obscure_dates(bcc_lesion, output_dir=output_dir)
            bcc_weight_emitter.write(bcc_lesion)
    bcc_weight_emitter.close()


def transform_height(item_paths, output_dir, project_id, observation_ids, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    bcc_height_emitter = emitter('bcc_height', output_dir=output_dir)
    for p,observation_type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            participantid = line.get('ParticipantID', line.get('participantid', None))
            observation = default_observation(participantid, project_id, line['date'], observation_type, line)
            observation_submitter_id = observation['submitter_id']
            height_submitter_id = '{}-bcc_height'.format(observation_submitter_id)
            if observation_submitter_id not in observation_ids:
                print('transform_height {} not in observation_ids, skipping.'.format(height_submitter_id))
                continue
            bcc_height = {
                'type': 'bcc_height',
                'project_id': project_id,
                'observation': {'submitter_id': observation_submitter_id},
                'submitter_id': height_submitter_id
                }
            line['source'] = source
            if callback:
                line = callback(line)
            bcc_height.update(line)
            bcc_height = obscure_dates(bcc_height, output_dir=output_dir)
            bcc_height_emitter.write(bcc_height)
    bcc_height_emitter.close()


def transform_biomarker(item_paths, output_dir, project_id, observation_ids, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    bcc_biomarker_emitter = emitter('bcc_biomarker', output_dir=output_dir)
    for p,observation_type, callback in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            participantid = line.get('ParticipantID', line.get('participantid', None))
            observation = default_observation(participantid, project_id, line['date'], observation_type, line)
            observation_submitter_id = observation['submitter_id']
            biomarker_submitter_id = '{}-bcc_biomarker'.format(observation_submitter_id)
            if observation_submitter_id not in observation_ids:
                print('transform_biomarker {} not in observation_ids, skipping.'.format(biomarker_submitter_id))
                continue
            bcc_biomarker = {
                'type': 'bcc_biomarker',
                'project_id': project_id,
                'observation': {'submitter_id': observation_submitter_id},
                'submitter_id': biomarker_submitter_id
                }
            line['source'] = source
            if callback:
                line = callback(line)
            bcc_biomarker.update(line)
            bcc_biomarker = obscure_dates(bcc_biomarker, output_dir=output_dir)
            bcc_biomarker_emitter.write(bcc_biomarker)
    bcc_biomarker_emitter.close()


def lookups():
    look_ups = {}
    for p in LOOKUP_PATHS:
        c = p.replace('source/bcc/','').replace('.json','')
        if c in ['lesion_site', 'lesion_procedure']:
            c = c.replace('lesion_','')
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
        if k in ['assay_type_id', 'not_available_reason_id', 'order_proc_id']:
            continue
        lup = k.replace('_id', '')
        if line[k]:
            try:
                line[lup] = LOOKUPS[lup][line[k]]
            except Exception as e:
                print('******')
                print(lup, k, line.get(k, None), line)
                print(LOOKUPS.get(lup, None))
                print('******')
                raise e
        del line[k]
    if 'chromosome' in line:
        line['chromosome'] = str(line['chromosome'].replace('chr',''))
    if 'gene' in line:
        line['gene_symbol'] = line['gene']
        del line['gene']

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
    if 'sizeaxis1' in schema['properties']:
        schema['properties']['lesion'] = {'$ref': '_definitions.yaml#/to_one'}
    return schema


if __name__ == "__main__":
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    item_paths = [
        ('source/bcc/lesion_size.json', 'Lesion', my_callback),
        ('source/bcc/vWeightMonthly.json', 'Weight', my_callback),
        ('source/bcc/weight_ohsu.json', 'Weight', my_callback),
        ('source/bcc/height_ohsu.json', 'Height', my_callback),
        ('source/bcc/vbiomarkermeasurement.json', 'Biomarker', my_callback),
        ('source/bcc/biomarker_measurement_ohsu.json', 'Biomarker', my_callback),
    ]
    observation_ids = transform_gen3(item_paths, output_dir=args.output_dir, project_id=args.project_id)

    item_paths = [
        ('source/bcc/lesion_size.json', 'Lesion', my_callback),
    ]
    transform_lesion(item_paths, observation_ids=observation_ids, output_dir=args.output_dir, project_id=args.project_id)
    item_paths = [
        'source/bcc/lesion_size.json',
    ]
    link = {'name':'observation', 'backref':'bcc_lesion', 'label':'describes', 'target_type':'observation',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_lesion', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)

    item_paths = [
        ('source/bcc/vWeightMonthly.json', 'Weight', my_callback),
        ('source/bcc/weight_ohsu.json', 'Weight', my_callback),
    ]
    transform_weight(item_paths, observation_ids=observation_ids, output_dir=args.output_dir, project_id=args.project_id)
    item_paths = [
        'source/bcc/vWeightMonthly.json',
    ]
    link = {'name':'observation', 'backref':'bcc_weight', 'label':'describes', 'target_type':'observation',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_weight', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)


    item_paths = [
        ('source/bcc/height_ohsu.json', 'Height', my_callback),
    ]
    transform_height(item_paths, observation_ids=observation_ids, output_dir=args.output_dir, project_id=args.project_id)

    item_paths = [
        'source/bcc/height_ohsu.json',
    ]
    link = {'name':'observation', 'backref':'bcc_height', 'label':'describes', 'target_type':'observation',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_height', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)

    item_paths = [
        ('source/bcc/vbiomarkermeasurement.json', 'Biomarker', my_callback),
        ('source/bcc/biomarker_measurement_ohsu.json', 'Biomarker', my_callback),
    ]
    transform_biomarker(item_paths, observation_ids=observation_ids, output_dir=args.output_dir, project_id=args.project_id)

    item_paths = [
        'output/bcc/bcc_biomarker.json',
    ]
    link = {'name':'observation', 'backref':'bcc_biomarker', 'label':'describes', 'target_type':'observation',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_biomarker', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)
