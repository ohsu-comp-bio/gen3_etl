"""Read bmeg ccle Biosample, Aliquote and writes gen3 json."""
import os
import json

from gen3_etl.utils.ioutils import reader, JSONEmitter
from gen3_etl.utils.cli import default_argument_parser

DEFAULT_OUTPUT_DIR = 'output/ccle'


def transform(output_dir, compresslevel=0):
    """Read bmeg json and writes gen3 json."""
    samples_emitter = JSONEmitter(os.path.join(output_dir, 'samples.json'), compresslevel=0)
    aliquots_emitter = JSONEmitter(os.path.join(output_dir, 'aliquots.json'), compresslevel=0)
    diagnosis_emitter = JSONEmitter(os.path.join(output_dir, 'diagnosis.json'), compresslevel=0)

    samples = {}
    diagnoses = {}

    for p in ['source/ccle/BiosampleFor.Edge.json.gz', 'source/ccle/maf.BiosampleFor.Edge.json.gz']:
        for line in reader(p):
            case_submitter_id = line['to']
            diagnosis_submitter_id = 'diagnosis-{}'.format(case_submitter_id)
            sample = {'type': 'sample', '*cases': {'submitter_id': line['to']}}
            sample['*submitter_id'] = line['from']
            sample['*diagnoses'] = {'submitter_id': diagnosis_submitter_id}

            samples[sample['*submitter_id']] = sample
            diagnosis = {
                'type': 'diagnosis',
                '*submitter_id': diagnosis_submitter_id,
                '*cases': {'submitter_id': case_submitter_id}
            }
            diagnoses[sample['*submitter_id']] = diagnosis

    for p in ['source/ccle/maf.AliquotFor.Edge.json.gz', 'source/ccle/AliquotFor.Edge.json.gz']:
        for line in reader(p):
            line = json.loads(line)
            # ['type', 'project_id', '*submitter_id', '*cases.submitter_id', 'diagnoses.submitter_id', 'biosample_anatomic_site', 'composition', 'current_weight', 'days_to_collection', 'days_to_sample_procurement', 'diagnosis_pathologically_confirmed', 'freezing_method', 'initial_weight', 'intermediate_dimension', 'is_ffpe', 'longest_dimension', 'method_of_sample_procurement', 'oct_embedded', 'preservation_method', 'sample_type', 'sample_type_id', 'sample_volume', 'shortest_dimension', 'time_between_clamping_and_freezing', 'time_between_excision_and_freezing', 'tissue_type', 'tumor_code', 'tumor_code_id', 'tumor_descriptor',]
            aliquot = {'type': 'aliquot', '*samples': {'submitter_id': line['to']}}
            aliquot['*submitter_id'] = line['from']
            aliquots_emitter.write(aliquot)

    for p in ['source/ccle/BioSample.Vertex.json.gz', 'source/ccle/maf.BioSample.Vertex.json.gz']:
        for line in reader(p):
            line = json.loads(line)
            # ['type', 'project_id', '*submitter_id', '*cases.submitter_id', 'diagnoses.submitter_id', 'biosample_anatomic_site', 'composition', 'current_weight', 'days_to_collection', 'days_to_sample_procurement', 'diagnosis_pathologically_confirmed', 'freezing_method', 'initial_weight', 'intermediate_dimension', 'is_ffpe', 'longest_dimension', 'method_of_sample_procurement', 'oct_embedded', 'preservation_method', 'sample_type', 'sample_type_id', 'sample_volume', 'shortest_dimension', 'time_between_clamping_and_freezing', 'time_between_excision_and_freezing', 'tissue_type', 'tumor_code', 'tumor_code_id', 'tumor_descriptor',]
            sample = samples[line['gid']]
            samples_emitter.write(sample)

            diagnosis = diagnoses[sample['*submitter_id']]
            ccle_attributes = line['data']
            diagnosis['*primary_diagnosis'] = ccle_attributes.get("Primary Disease", ccle_attributes.get("Subtype Disease", 'unknown'))

            diagnosis['*age_at_diagnosis'] = None
            diagnosis['*classification_of_tumor'] = 'Unknown'
            diagnosis['*days_to_last_follow_up'] = None
            diagnosis['*days_to_last_known_disease_status'] = None
            diagnosis['*days_to_recurrence'] = None
            diagnosis['*last_known_disease_status'] = 'Unknown tumor status'
            diagnosis['*morphology'] = 'unknown'
            diagnosis['*progression_or_recurrence'] = 'unknown'
            diagnosis['*site_of_resection_or_biopsy'] = 'unknown'
            diagnosis['*tissue_or_organ_of_origin'] = 'unknown'
            diagnosis['*tumor_grade'] = 'unknown'
            diagnosis['*tumor_stage'] = 'unknown'
            diagnosis['*vital_status'] = 'unknown'

            diagnosis_emitter.write(diagnosis)

    samples_emitter.close()
    aliquots_emitter.close()
    diagnosis_emitter.close()


if __name__ == "__main__":
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads bcc tsv and writes gen3 json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )

    args = parser.parse_args()
    transform(output_dir=args.output_dir)
