"""A transformer for gen3 project,reads cases bcc, writes to DEFAULT_OUTPUT_DIR."""
import hashlib
import os

from bcc.utils.ioutils import reader, JSONEmitter
from bcc.utils.cli import default_argument_parser


DEFAULT_OUTPUT_DIR = 'output/bcc'
DEFAULT_EXPERIMENT_CODE = 'atac'


def submitter_id(line):
    """Obscure MRN."""
    return hashlib.md5(line['MRN'].encode('utf-8')).hexdigest()


def download(output_dir, experiment_code, compresslevel=0):
    """Read bcc tsv and writes gen3 json."""
    cases_emitter = JSONEmitter(os.path.join(output_dir, 'cases.json'), compresslevel=0)
    demographics_emitter = JSONEmitter(os.path.join(output_dir, 'demographics.json'), compresslevel=0)
    diagnosis_emitter = JSONEmitter(os.path.join(output_dir, 'diagnosis.json'), compresslevel=0)
    # ['MRN', 'OPTR', 'Date Of Initial Diagnosis', 'Sequence Number', 'Cancer Status', 'cEarliest Chemo Date', 'cEarliest Chemo Date Source', 'cErrorList', 'cEventCount', 'cNeoadjuvant Treatment', 'Count', 'cParent Specimen Count', 'Date of Most Definitive Surgical Resection', 'Tumor Size', 'Type Of First Recurrence', 'Case_ICD::Transformation', 'Case_Patient::Sex']
    for line in reader('source/bcc/bcc-cases.tsv'):
        # ['type', 'project_id', '*submitter_id', '*cases.submitter_id', 'ethnicity', 'gender', 'race', 'year_of_birth', 'year_of_death']
        case = {'type': 'case', '*experiments': {'submitter_id': experiment_code}}
        case_submitter_id = submitter_id(line)
        case['*submitter_id'] = case_submitter_id
        case['disease_type'] = line['Case_ICD::Transformation']
        cases_emitter.write(case)

        # type	project_id	*submitter_id	*cases.submitter_id	ethnicity	gender	race	year_of_birth	year_of_death
        demographic = {'type': 'demographic', '*submitter_id': 'demographic-{}'.format(case_submitter_id), '*cases': {'submitter_id': case_submitter_id}}
        demographic['gender'] = line['Case_Patient::Sex'].lower()
        if demographic['gender'] == '':
            demographic['gender'] = 'unknown'
        demographics_emitter.write(demographic)

        # ['type', 'project_id', 'submitter_id', 'cases.submitter_id',
        # '*age_at_diagnosis', '*classification_of_tumor', '*days_to_last_follow_up', '*days_to_last_known_disease_status', '*days_to_recurrence', '*last_known_disease_status', '*morphology', '*primary_diagnosis', '*progression_or_recurrence', '*site_of_resection_or_biopsy', '*tissue_or_organ_of_origin', '*tumor_grade', '*tumor_stage', '*vital_status', # 'ajcc_clinical_m', 'ajcc_clinical_n', 'ajcc_clinical_stage', 'ajcc_clinical_t',
        # 'ajcc_pathologic_m', 'ajcc_pathologic_n', 'ajcc_pathologic_stage', 'ajcc_pathologic_t', 'ann_arbor_b_symptoms', 'ann_arbor_clinical_stage', 'ann_arbor_extranodal_involvement', 'ann_arbor_pathologic_stage', 'burkitt_lymphoma_clinical_variant', 'cause_of_death', 'circumferential_resection_margin', 'colon_polyps_history', 'days_to_birth', 'days_to_death', 'days_to_hiv_diagnosis', 'days_to_new_event', 'figo_stage', 'hiv_positive', 'hpv_positive_type', 'hpv_status', 'laterality',
        # 'ldh_level_at_diagnosis', 'ldh_normal_range_upper', 'lymph_nodes_positive', 'lymphatic_invasion_present', 'method_of_diagnosis', 'new_event_anatomic_site', 'new_event_type', 'perineural_invasion_present', 'prior_malignancy', 'prior_treatment', 'residual_disease', 'vascular_invasion_present', 'year_of_diagnosis']
        diagnosis = {'type': 'diagnosis', '*submitter_id': 'diagnosis-{}'.format(case_submitter_id), '*cases': {'submitter_id': case_submitter_id}}
        diagnosis['*age_at_diagnosis'] = None
        diagnosis['*classification_of_tumor'] = 'Unknown'  # ['primary', 'metastasis', 'recurrence', 'other', 'Unknown', 'not reported', 'Not Allowed To Collect']
        diagnosis['*days_to_last_follow_up'] = None
        diagnosis['*days_to_last_known_disease_status'] = None
        diagnosis['*days_to_recurrence'] = None
        # [ 'Distant met recurrence/progression',
        # 'Loco-regional recurrence/progression',
        # 'Biochemical evidence of disease without structural correlate',
        # 'Tumor free',
        # 'Unknown tumor status',
        # 'With tumor',
        # 'not reported',
        # 'Not Allowed To Collect']
        disease_status = {
            'Evidence of this tumor': 'With tumor',
            'No evidence of this tumor': 'Tumor free',
            'Unknown, indeterminate whether this tumor is present; not stated': 'Unknown tumor status'
        }

        diagnosis['*last_known_disease_status'] = disease_status.get(line['Cancer Status'], 'Unknown tumor status')
        diagnosis['*morphology'] = 'tumor_size={}'.format(line['Tumor Size'])  # "None is not of type 'string'")
        diagnosis['*primary_diagnosis'] = line['Case_ICD::Transformation']
        diagnosis['*progression_or_recurrence'] = 'unknown'   # ['yes', 'no', 'unknown', 'not reported', 'Not Allowed To Collect']
        diagnosis['*site_of_resection_or_biopsy'] = 'unknown'
        diagnosis['*tissue_or_organ_of_origin'] = 'pancrease'
        diagnosis['*tumor_grade'] = 'unknown'  # "None is not of type 'string'")
        diagnosis['*tumor_stage'] = 'unknown'  # "None is not of type 'string'")
        diagnosis['*vital_status'] = 'unknown'

        diagnosis_emitter.write(diagnosis)

    cases_emitter.close()
    demographics_emitter.close()
    diagnosis_emitter.close()


if __name__ == "__main__":
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads bcc tsv and writes gen3 json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )
    parser.add_argument('--experiment_code', type=str,
                        default=DEFAULT_EXPERIMENT_CODE,
                        help='Name of existing gen3 experiment ({}).'.format(DEFAULT_EXPERIMENT_CODE))

    args = parser.parse_args()
    download(output_dir=args.output_dir, experiment_code=args.experiment_code)
