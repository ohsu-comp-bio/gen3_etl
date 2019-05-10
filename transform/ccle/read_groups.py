"""Read bmeg ccle CallsetFor writes gen3 json."""
import os
import json

from gen3_etl.utils.ioutils import reader, JSONEmitter
from gen3_etl.utils.cli import default_argument_parser

DEFAULT_OUTPUT_DIR = 'output/ccle'

PATHS = {}


def transform(output_dir, compresslevel=0):
    """Read bmeg json and writes gen3 json."""
    read_groups_emitter = JSONEmitter(os.path.join(output_dir, 'read_group.json'), compresslevel=0)
    read_groups = {}

    # [ "_id", "data", "from", "gid", "label", "to" ]
    # {"_id": "(Callset:ccle:ACH-001270:None)--CallsetFor->(Aliquot:ACH-001270)", "gid": "(Callset:ccle:ACH-001270:None)--CallsetFor->(Aliquot:ACH-001270)", "label": "CallsetFor", "from": "Callset:ccle:ACH-001270:None", "to": "Aliquot:ACH-001270", "data": {}}
    for line in reader('source/ccle/maf.CallsetFor.Edge.json.gz'):
        # *type	project_id	*submitter_id	*aliquots.submitter_id	RIN	adapter_name	adapter_sequence	barcoding_applied	base_caller_name	base_caller_version	experiment_name	flow_cell_barcode	includes_spike_ins	instrument_model	is_paired_end	library_name	library_preparation_kit_catalog_number	library_preparation_kit_name	library_preparation_kit_vendor	library_preparation_kit_version	library_selection	library_strand	library_strategy	platform	read_group_name	read_length	sequencing_center	sequencing_date	size_selection_range	spike_ins_concentration	spike_ins_fasta	target_capture_kit_catalog_number	target_capture_kit_name	target_capture_kit_target_region	target_capture_kit_vendor	target_capture_kit_version	to_trim_adapter_sequence ]
        read_group_submitter_id = 'read_group-{}'.format(line['from'])
        if read_group_submitter_id in read_groups:
            continue
        read_group = {'type': 'read_group', '*aliquots': {'submitter_id': line['to']}}
        read_group['*submitter_id'] = read_group_submitter_id
        read_groups[read_group_submitter_id] = read_group

    for read_group in read_groups:
        read_groups_emitter.write(read_groups[read_group])

    read_groups_emitter.close()


if __name__ == "__main__":
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads bmeg callset json and writes gen3 read_group json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )

    args = parser.parse_args()
    transform(output_dir=args.output_dir)
