"""A transformer for gen3 project,reads specimens bcc, writes to DEFAULT_OUTPUT_DIR."""
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
    samples_emitter = JSONEmitter(os.path.join(output_dir, 'samples.json'), compresslevel=0)
    # [ 'b_isDuplicate', 'b_isParentsample', 'b_isPDMC', 'b_isPresFormat', 'Date', 'MRN', 'OPTR', 'Record Source', 'sample ID', 'Status', 'Type', 'ID', 'ID_Parent', 'Multikey ID', 'PDMC Type', 'Percent Viable', 'Preservation Format', 'Surgical Pathology Code', 'Total Cells', 'Vials Remaining', 'xPreservation Format', 'z_CreatedTS', 'z_Creator', 'z_ModifiedTS', 'z_Modifier', ]
    for line in reader('source/bcc/specimens.tsv'):
        # ['type', 'project_id', '*submitter_id', '*cases.submitter_id', 'diagnoses.submitter_id', 'biosample_anatomic_site', 'composition', 'current_weight', 'days_to_collection', 'days_to_sample_procurement', 'diagnosis_pathologically_confirmed', 'freezing_method', 'initial_weight', 'intermediate_dimension', 'is_ffpe', 'longest_dimension', 'method_of_sample_procurement', 'oct_embedded', 'preservation_method', 'sample_type', 'sample_type_id', 'sample_volume', 'shortest_dimension', 'time_between_clamping_and_freezing', 'time_between_excision_and_freezing', 'tissue_type', 'tumor_code', 'tumor_code_id', 'tumor_descriptor',]
        sample = {'type': 'sample', '*cases': {'submitter_id': case_submitter_id(line)}, '*diagnoses': {'submitter_id': 'diagnosis-{}'.format(case_submitter_id(line))}}
        sample['*submitter_id'] = line['ID']
        sample['biospecimen_anatomic_site'] = 'Pancreas'
        samples_emitter.write(sample)
    samples_emitter.close()


if __name__ == "__main__":
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads bcc tsv and writes gen3 json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )
    args = parser.parse_args()
    transform(output_dir=args.output_dir)
