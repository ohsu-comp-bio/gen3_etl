"""A transformer for gen3 project,reads gene_trails_cnv bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os

from bcc.utils.ioutils import reader, JSONEmitter
from bcc.utils.cli import default_argument_parser


DEFAULT_OUTPUT_DIR = 'output/bcc'


def case_submitter_id(line):
    """Obscure MRN."""
    return hashlib.md5(line['MRN'].encode('utf-8')).hexdigest()


def transform(output_dir, compresslevel=0):
    """Read bcc tsv and writes gen3 json."""
    read_groups_emitter = JSONEmitter(os.path.join(output_dir, 'read_group.json'), compresslevel=0)
    ssm_emitter = JSONEmitter(os.path.join(output_dir, 'submitted_somatic_mutation.json'), compresslevel=0)
    read_groups = {}
    ssms = {}

    for line in reader('source/bcc/gene_trails_cnv.tsv'):
        # *type	project_id	*submitter_id	*aliquots.submitter_id	RIN	adapter_name	adapter_sequence	barcoding_applied	base_caller_name	base_caller_version	experiment_name	flow_cell_barcode	includes_spike_ins	instrument_model	is_paired_end	library_name	library_preparation_kit_catalog_number	library_preparation_kit_name	library_preparation_kit_vendor	library_preparation_kit_version	library_selection	library_strand	library_strategy	platform	read_group_name	read_length	sequencing_center	sequencing_date	size_selection_range	spike_ins_concentration	spike_ins_fasta	target_capture_kit_catalog_number	target_capture_kit_name	target_capture_kit_target_region	target_capture_kit_vendor	target_capture_kit_version	to_trim_adapter_sequence ]
        read_group_submitter_id = 'read_group-{}'.format(line['ID_Assay'])
        if read_group_submitter_id in read_groups:
            continue
        read_group = {'type': 'read_group', '*aliquots': {'submitter_id': line['ID_Assay']}}
        read_group['*submitter_id'] = read_group_submitter_id
        read_groups[read_group_submitter_id] = read_group

        # type	project_id	*submitter_id	core_metadata_collections.submitter_id#1	read_groups.submitter_id#1	*data_category	*data_format	*data_type	*experimental_strategy	*file_name	*file_size	*md5sum	object_id	state_comment	total_variants	urls
        ssm_submitter_id = 'cnv-{}'.format(line['ID_Assay'])
        ssm = {'type': 'submitted_somatic_mutation', '*read_groups': {'submitter_id': read_group_submitter_id}}
        ssm['*submitter_id'] = ssm_submitter_id
        ssm['md5sum'] = '01234567890123456789012345678901'
        ssm['file_size'] = 1000
        ssm['file_name'] = '{}.cnv.tsv'.format(line['ID_Assay'])
        ssm['experimental_strategy'] = 'gene trails'
        ssm['data_type'] = 'maf like'
        ssm['data_format'] = 'tsv'
        ssm['data_category'] = 'omics'
        ssms[ssm_submitter_id] = ssm

    for line in reader('source/bcc/gene_trails_variants.tsv'):
        read_group_submitter_id = 'read_group-{}'.format(line['ID_Assay'])
        read_group = {'type': 'read_group', '*aliquots': {'submitter_id': line['ID_Assay']}}
        read_group['*submitter_id'] = read_group_submitter_id
        read_groups[read_group_submitter_id] = read_group

        ssm = {'type': 'submitted_somatic_mutation', '*read_groups': {'submitter_id': read_group_submitter_id}}
        ssm_submitter_id = 'snp-{}'.format(line['ID_Assay'])
        ssm['*submitter_id'] = ssm_submitter_id
        ssm['md5sum'] = '01234567890123456789012345678901'
        ssm['file_size'] = 1000
        ssm['file_name'] = '{}.snp.tsv'.format(line['ID_Assay'])
        ssm['experimental_strategy'] = 'gene trails'
        ssm['data_type'] = 'maf like'
        ssm['data_format'] = 'tsv'
        ssm['data_category'] = 'omics'
        ssms[ssm_submitter_id] = ssm

    for read_group in read_groups:
        read_groups_emitter.write(read_groups[read_group])
    for ssm in ssms:
        ssm_emitter.write(ssms[ssm])

    read_groups_emitter.close()
    ssm_emitter.close()


if __name__ == "__main__":
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads bcc tsv and writes gen3 json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )

    args = parser.parse_args()
    transform(output_dir=args.output_dir)
