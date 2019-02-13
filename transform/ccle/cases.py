"""A transformer for gen3 project, experiment and case from ccle, writes to DEFAULT_OUTPUT_DIR."""
import os
import json

from gen3_etl.utils.ioutils import reader, JSONEmitter
from gen3_etl.utils.cli import default_argument_parser


DEFAULT_OUTPUT_DIR = 'output/ccle'
DEFAULT_EXPERIMENT = 'ccle'


def transform(output_dir, compresslevel=0):
    """Transform the bmeg input to gen3 output directory."""
    projects_emitter = JSONEmitter(os.path.join(output_dir, 'projects.json'), compresslevel=0)
    experiments_emitter = JSONEmitter(os.path.join(output_dir, 'experiments.json'), compresslevel=0)
    cases_emitter = JSONEmitter(os.path.join(output_dir, 'cases.json'), compresslevel=0)
    demographics_emitter = JSONEmitter(os.path.join(output_dir, 'demographics.json'), compresslevel=0)
    cases = {}
    projects = {}
    experiments = {}

    for p in ['source/ccle/InProject.Edge.json.gz', 'source/ccle/maf.InProject.Edge.json.gz']:
        for line in reader(p):
            line = json.loads(line)
            # # ['type', 'project_id', '*submitter_id', '*cases.submitter_id', 'ethnicity', 'gender', 'race', 'year_of_birth', 'year_of_death']
            project_submitter_id = line['to']
            project_name = project_submitter_id.replace('Project:', '')
            project_name = 'ccle'
            project = {'type': 'project', "code": project_name, "name": project_name, "state": "open", "availability_type": "Open", "dbgap_accession_number": project_name}
            projects[project_name] = project

            experiment_submitter_id = "experiment-{}".format(project_submitter_id.replace('Project:', ''))
            experiment = {"type": "experiment", "projects": [{"code": project_name}], "submitter_id": experiment_submitter_id}
            experiment["experimental_description"] = project_submitter_id.replace('Project:', '')
            experiments[experiment_submitter_id] = experiment

            case = {'type': 'case', '*experiments': {'submitter_id': experiment_submitter_id}}
            case_submitter_id = line['from']
            case['submitter_id'] = case_submitter_id
            cases[case_submitter_id] = case

    for project in projects:
        projects_emitter.write(projects[project])
    for experiment in experiments:
        experiments_emitter.write(experiments[experiment])

    projects_emitter.close()
    experiments_emitter.close()

    for p in ['source/ccle/Individual.Vertex.json.gz', 'source/ccle/maf.Individual.Vertex.json.gz']:
        # ['MRN', 'OPTR', 'Date Of Initial Diagnosis', 'Sequence Number', 'Cancer Status', 'cEarliest Chemo Date', 'cEarliest Chemo Date Source', 'cErrorList', 'cEventCount', 'cNeoadjuvant Treatment', 'Count', 'cParent Specimen Count', 'Date of Most Definitive Surgical Resection', 'Tumor Size', 'Type Of First Recurrence', 'Case_ICD::Transformation', 'Case_Patient::Sex']
        for line in reader(p):
            # {"_id": "Individual:CCLE:ACH-001665", "gid": "Individual:CCLE:ACH-001665", "label": "Individual", "data": {"individual_id": "CCLE:ACH-001665", "ccle_attributes": {"gender": "Male"}}}
            line = json.loads(line)
            case_submitter_id = line['gid']
            # # ['type', 'project_id', '*submitter_id', '*cases.submitter_id', 'ethnicity', 'gender', 'race', 'year_of_birth', 'year_of_death']
            case = cases[case_submitter_id]
            cases_emitter.write(case)
            #
            # # type	project_id	*submitter_id	*cases.submitter_id	ethnicity	gender	race	year_of_birth	year_of_death
            demographic = {'type': 'demographic', '*submitter_id': 'demographic-{}'.format(case_submitter_id), '*cases': {'submitter_id': case_submitter_id}}
            data = line['data']
            demographic['gender'] = data.get('gender', 'unknown').lower()
            if demographic['gender'] not in ['male', 'female']:
                demographic['gender'] = 'unknown'
            demographics_emitter.write(demographic)
            #
            # # ['type', 'project_id', 'submitter_id', 'cases.submitter_id',
            # # '*age_at_diagnosis', '*classification_of_tumor', '*days_to_last_follow_up', '*days_to_last_known_disease_status', '*days_to_recurrence', '*last_known_disease_status', '*morphology', '*primary_diagnosis', '*progression_or_recurrence', '*site_of_resection_or_biopsy', '*tissue_or_organ_of_origin', '*tumor_grade', '*tumor_stage', '*vital_status', # 'ajcc_clinical_m', 'ajcc_clinical_n', 'ajcc_clinical_stage', 'ajcc_clinical_t',
            # # 'ajcc_pathologic_m', 'ajcc_pathologic_n', 'ajcc_pathologic_stage', 'ajcc_pathologic_t', 'ann_arbor_b_symptoms', 'ann_arbor_clinical_stage', 'ann_arbor_extranodal_involvement', 'ann_arbor_pathologic_stage', 'burkitt_lymphoma_clinical_variant', 'cause_of_death', 'circumferential_resection_margin', 'colon_polyps_history', 'days_to_birth', 'days_to_death', 'days_to_hiv_diagnosis', 'days_to_new_event', 'figo_stage', 'hiv_positive', 'hpv_positive_type', 'hpv_status', 'laterality',
            # # 'ldh_level_at_diagnosis', 'ldh_normal_range_upper', 'lymph_nodes_positive', 'lymphatic_invasion_present', 'method_of_diagnosis', 'new_event_anatomic_site', 'new_event_type', 'perineural_invasion_present', 'prior_malignancy', 'prior_treatment', 'residual_disease', 'vascular_invasion_present', 'year_of_diagnosis']
            # diagnosis = {'type': 'diagnosis', '*submitter_id': 'diagnosis-{}'.format(case_submitter_id),  '*cases': {'submitter_id': case_submitter_id}}
            # diagnosis['*age_at_diagnosis'] = None
            # diagnosis['*classification_of_tumor'] = 'Unknown' # ['primary', 'metastasis', 'recurrence', 'other', 'Unknown', 'not reported', 'Not Allowed To Collect']
            # diagnosis['*days_to_last_follow_up'] = None
            # diagnosis['*days_to_last_known_disease_status'] = None
            # diagnosis['*days_to_recurrence'] = None
            # # [ 'Distant met recurrence/progression',
            # # 'Loco-regional recurrence/progression',
            # # 'Biochemical evidence of disease without structural correlate',
            # # 'Tumor free',
            # # 'Unknown tumor status',
            # # 'With tumor',
            # # 'not reported',
            # # 'Not Allowed To Collect']
            # disease_status = {
            #     'Evidence of this tumor': 'With tumor',
            #     'No evidence of this tumor': 'Tumor free',
            #     'Unknown, indeterminate whether this tumor is present; not stated': 'Unknown tumor status'
            # }
            #
            # diagnosis['*last_known_disease_status'] = disease_status.get(line['Cancer Status'], 'Unknown tumor status')
            # diagnosis['*morphology'] = 'tumor_size={}'.format(line['Tumor Size']) # "None is not of type 'string'")
            # diagnosis['*primary_diagnosis'] = line['Case_ICD::Transformation']
            # diagnosis['*progression_or_recurrence'] = 'unknown' # ['yes', 'no', 'unknown', 'not reported', 'Not Allowed To Collect']
            # diagnosis['*site_of_resection_or_biopsy'] = 'unknown'
            # diagnosis['*tissue_or_organ_of_origin'] = 'pancrease'
            # diagnosis['*tumor_grade'] = 'unknown' #  "None is not of type 'string'")
            # diagnosis['*tumor_stage'] = 'unknown' #  "None is not of type 'string'")
            # diagnosis['*vital_status'] = 'unknown'
            #
            # diagnosis_emitter.write(diagnosis)

    cases_emitter.close()
    demographics_emitter.close()


if __name__ == "__main__":
    parser = default_argument_parser(
        output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads ccle bmeg vertexes and writes gen3 json ({}).'.format(DEFAULT_OUTPUT_DIR)
    )

    args = parser.parse_args()
    transform(output_dir=args.output_dir)
