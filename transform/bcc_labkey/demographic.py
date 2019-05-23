"""A transformer for gen3 project,reads demographics bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader
from gen3_etl.utils.cli import default_argument_parser

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter
from gen3_etl.utils.schema import generate, template


def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    demographics_emitter = emitter('demographic', output_dir=output_dir)
    bcc_demographics_emitter = emitter('bcc_demographic', output_dir=output_dir)
    demographics = {}
    bcc_demographics = {}

    for p in item_paths:
        source = os.path.splitext(os.path.basename(p))[0]
        for line in reader(p):
            case_submitter_id = line['participantid']
            submitter_id = '{}-demographic'.format(case_submitter_id)
            bcc_submitter_id = '{}-{}'.format(submitter_id, source)

            demographic = {'type': 'demographic', 'cases': {'submitter_id': case_submitter_id }, 'submitter_id': submitter_id, 'project_id': DEFAULT_PROJECT_ID}
            bcc_demographic = {'type': 'bcc_demographic', 'demographic': {'submitter_id': submitter_id}, 'source': source, 'submitter_id': bcc_submitter_id, 'project_id': DEFAULT_PROJECT_ID}
            demographics[submitter_id] = demographic
            if bcc_submitter_id in bcc_demographics:
                bcc_demographic = bcc_demographics[bcc_submitter_id]
            bcc_demographic.update(line)
            bcc_demographics[bcc_submitter_id] = bcc_demographic
    for k in demographics:
        demographics_emitter.write(demographics[k])
    demographics_emitter.close()
    for k in bcc_demographics:
        bcc_demographics_emitter.write(bcc_demographics[k])
    bcc_demographics_emitter.close()


if __name__ == "__main__":
    item_paths = ['source/bcc/vDemoGraphicsForPlot.json']
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()
    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    p = os.path.join(args.output_dir, 'demographic.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)
    p = os.path.join(args.output_dir, 'bcc_demographic.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)

    if args.schema:

        def my_callback(schema):
            # adds the source property
            schema['properties']['source'] = {'type': 'string'}
            schema['category'] = 'bcc extention'
            schema['properties']['demographic'] = {'$ref': '_definitions.yaml#/to_one'}
            return schema

        link = {'name':'demographic', 'backref':'bcc_demographic', 'label':'extends', 'target_type':'demographic',  'multiplicity': 'many_to_one', 'required': False }
        schema_path = generate(item_paths,'bcc_demographic', output_dir='output/bcc', links=[link], callback=my_callback)
        assert os.path.isfile(p), 'should have an schema file {}'.format(schema_path)
        print(schema_path)
