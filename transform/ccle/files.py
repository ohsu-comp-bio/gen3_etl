"""Read bmeg ccle File writes gen3 json."""
import os
import json

from gen3_etl.utils.ioutils import reader, JSONEmitter
from gen3_etl.utils.cli import default_argument_parser

DEFAULT_OUTPUT_DIR = 'output/ccle'


def transform(output_dir, compresslevel=0):
    """Read bmeg json and writes gen3 json."""
    ssm_emitter = JSONEmitter(os.path.join(output_dir, 'submitted_somatic_mutation.json'), compresslevel=0)
    read_groups = {}

    # [ "_id", "data", "from", "gid", "label", "to" ]
    for line in reader('source/ccle/DerivedFrom.Edge.json.gz'):
        line = json.loads(line)
        read_groups[line['from']] = 'read_group-{}'.format(line['to'])

    for line in reader('source/ccle/File.Vertex.json.gz'):
        line = json.loads(line)
        ssm_submitter_id = line['gid']
        read_group_submitter_id = read_groups[ssm_submitter_id]
        ssm = {
            'type': 'submitted_somatic_mutation',
            '*read_groups': {
                'submitter_id': read_group_submitter_id
            }
        }
        ssm['*submitter_id'] = ssm_submitter_id
        ssm['md5sum'] = line['data']['md5']
        ssm['file_size'] = line['data']['size']
        ssm['file_name'] = line['data']['path']
        ssm['experimental_strategy'] = 'etl'
        ssm['data_type'] = 'maf like'
        ssm['data_format'] = 'tsv'
        ssm['data_category'] = 'omics'
        ssm_emitter.write(ssm)

    ssm_emitter.close()


if __name__ == "__main__":
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads bmeg Files json and writes gen3 submitted_somatic_mutation json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )

    args = parser.parse_args()
    transform(output_dir=args.output_dir)
