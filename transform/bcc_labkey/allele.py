"""A transformer for gen3 project,reads alleles bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, default_parser, emitter
from gen3_etl.utils.schema import generate, template

LOOKUP_PATHS = """
source/bcc/genetrails_classification.json
source/bcc/genetrails_copy_number_result_type.json
source/bcc/genetrails_protein_variant_type.json
source/bcc/genetrails_result_significance.json
source/bcc/genetrails_result_type.json
source/bcc/genetrails_run_status.json
source/bcc/genetrails_transcript_priority.json
source/bcc/genetrails_variant_type.json
source/bcc/chromosome.json
source/bcc/assay_categories.json
source/bcc/assay_version.json
source/bcc/gene.json
source/bcc/genome_build.json
""".strip().split()


def transform(item_paths, output_dir, experiment_code, compresslevel=0, callback=None):
    """Read bcc labkey json and writes gen3 json."""
    alleles_emitter = emitter('allele', output_dir=output_dir)
    alleles = {}
    for p in item_paths:
        for line in reader(p):
            if callback:
                line = callback(line)
            allele = {
                'type': 'allele',
                'aliquots': {'submitter_id': '{}-aliquot'.format(line['sample_code'])},
                'projects': {'code': 'reference'},
                'submitter_id': line['lsid']}
            if line['lsid'] in alleles:
                allele = alleles[line['lsid']]
            allele.update(line)
            alleles[line['lsid']] = allele
    for k in alleles:
        alleles_emitter.write(alleles[k])
    alleles_emitter.close()


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
    if 'chromosome' in line:
        line['chromosome'] = str(line['chromosome'].replace('chr',''))
    return line


def my_pre_processor(schema):
    """Remove fields that start with _, fix key names with embedded /, fix id lookups """
    for k in [k for k in schema['properties'] if k.startswith('_')]:
        del schema['properties'][k]
    for k in [k for k in schema['properties'] if '/' in k]:
        schema['properties'][k.split('/')[1]] = schema['properties'][k]
        del schema['properties'][k]
    for k in [k for k in schema['properties'] if k.endswith('_id')]:
        schema['properties'][k.replace('_id', '')] = {'type': ['string', "'null'"]}  # schema['properties'][k]
        del schema['properties'][k]

    return schema


if __name__ == "__main__":
    item_paths = ['source/bcc/sample_genetrails_copy_number_variant.json','source/bcc/sample_genetrails_sequence_variant.json']
    args = default_parser().parse_args()
    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code, callback=my_callback)

    # glob.glob("output/bcc/*.json")
    if args.schema:
        schema_path = generate(item_paths,'allele', output_dir='output/bcc', callback=my_pre_processor)
        print(schema_path)
