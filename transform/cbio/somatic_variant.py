"""A transformer for gen3 project,reads somatic_variants bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os
import json

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID, default_parser, emitter
from gen3_etl.utils.schema import generate, template


def transform(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read medable csv and writes gen3 json."""
    somatic_variants_emitter = emitter('somatic_variants2', output_dir=output_dir)
    for line in reader(item_paths[0]):
        line['aliquot'] = {'submitter_id': line['aliquot']}
        line['submitter_id'] = '{}-{}-{}'.format(line['aliquot'], line['allele_id'], line['ensembl_transcript'])
        line['type'] = 'somatic_variant'
        del line['ensembl_transcript']
        del line['allele_id']
        somatic_variants_emitter.write(line)
    somatic_variants_emitter.close()


if __name__ == "__main__":
    item_paths = ['output/cbio/somatic_variant.json.gz']
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    transform(item_paths, output_dir=args.output_dir, experiment_code=args.experiment_code)

    p = os.path.join(args.output_dir, 'somatic_variants2.json')
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    print(p)
