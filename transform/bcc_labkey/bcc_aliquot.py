"""A transformer for gen3 project,reads bcc_aliquots bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter
from gen3_etl.utils.schema import generate, template

LOOKUP_PATHS = """
source/bcc/sample_type.json
""".strip().split()


def transform(item_paths, output_dir, experiment_code, compresslevel=0, callback=None):
    """Read bcc labkey json and writes gen3 json."""
    bcc_aliquot_emitter = emitter('bcc_aliquot', output_dir=output_dir)

    for p in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            line['source'] = source
            if callback:
                line = callback(line)
            bcc_aliquot = {
                'type': 'bcc_aliquot',
                'project_id': DEFAULT_PROJECT_ID,
                'aliquot': {'submitter_id': '{}-aliquot'.format(line['sample_code'])},
                'submitter_id': line['lsid']}
            bcc_aliquot.update(line)
            bcc_aliquot_emitter.write(bcc_aliquot)
    bcc_aliquot_emitter.close()


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

    return line


def my_schema_callback(schema):
    """Remove fields that start with _, fix key names with embedded /, fix id lookups """
    for k in [k for k in schema['properties'] if k.startswith('_')]:
        del schema['properties'][k]
    for k in [k for k in schema['properties'] if '/' in k]:
        schema['properties'][k.split('/')[1]] = schema['properties'][k]
        del schema['properties'][k]
    for k in [k for k in schema['properties'] if k.endswith('_id')]:
        if k in ['project_id', 'submitter_id']:
            continue
        schema['properties'][k.replace('_id', '')] = {'type': ['string', "'null'"]}  # schema['properties'][k]
        del schema['properties'][k]
    # adds the source property
    schema['category'] = 'bcc extention'
    schema['properties']['aliquot'] = {'$ref': '_definitions.yaml#/to_one'}
    return schema

    return schema


if __name__ == "__main__":
    item_paths = ['source/bcc/sample.json']
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()
    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code, callback=my_callback)

    link = {'name':'aliquot', 'backref':'bcc_aliquot', 'label':'derived_from', 'target_type':'aliquot',  'multiplicity': 'many_to_one', 'required': False }
    schema_path = generate(item_paths,'bcc_aliquot', output_dir='output/bcc', links=[link], callback=my_schema_callback)
    assert os.path.isfile(schema_path), 'should have an schema file {}'.format(schema_path)
    print(schema_path)
