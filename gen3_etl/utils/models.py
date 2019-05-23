import json
import hashlib


def default_case(experiment_code, participantid, project_id):
    """Creates a minimal case."""
    return {
        "type": "case",
        "experiments": {"submitter_id": experiment_code},
        "submitter_id": participantid,
        "primary_site": "unknown",
        "disease_type": "unknown",
        "project_id": project_id
    }


def default_diagnosis(participantid, project_id, line={}):
    """Creates a minimal diagnosis."""
    diagnosis = {
        'type': 'diagnosis',
        'submitter_id': '{}-diagnosis'.format(participantid),
        'cases': {'submitter_id': participantid},
        'project_id': project_id
    }
    diagnosis['tumor_stage'] = line.get('stage', 'unknown')
    if not diagnosis['tumor_stage']:
        diagnosis['tumor_stage'] = 'unkown'
    diagnosis['tumor_grade'] = line.get('definitive_grade_at_dx', 'unknown')
    if not diagnosis['tumor_grade']:
        diagnosis['tumor_grade'] = 'unkown'
    diagnosis['tissue_or_organ_of_origin'] = line.get('site', 'unknown')
    diagnosis['primary_diagnosis'] = line.get('diagnosis', 'unknown')
    diagnosis['morphology'] = "Unknown"
    diagnosis['last_known_disease_status'] = "Unknown tumor status"
    diagnosis['classification_of_tumor'] = "Unknown"
    diagnosis["vital_status"] = "unknown"
    diagnosis["progression_or_recurrence"] = "unknown"
    for p in ['days_to_recurrence', 'days_to_last_known_disease_status', 'days_to_last_follow_up', 'age_at_diagnosis']:
        diagnosis[p] = None
    for p in ["site_of_resection_or_biopsy"]:
        diagnosis[p] = "Unknown"
    for p in ['classification_of_tumor', 'morphology', 'primary_diagnosis',
              'site_of_resection_or_biopsy', 'tissue_or_organ_of_origin', 'tumor_grade', 'tumor_stage']:
        diagnosis[p] = "Unknown"
    return diagnosis


def default_sample(participantid, project_id, line={}):
    sample = { "type": "sample",
        "cases": {"submitter_id": participantid},
        "diagnoses": {"submitter_id": '{}-diagnosis'.format(participantid)},
        "project_id": project_id
    }
    if 'bems_id' in line:
        sample['submitter_id'] = '{}-biolibrary'.format(line['bems_id'])
    if 'oncolog_key' in line:
        sample['submitter_id'] = '{}-oncolog'.format(line['oncolog_key'])
    if 'Comment' in line:
        c = line['date'].split(' ')[0]
        if line['Comment']:
            c +=  '-' + line['Comment'].replace('OCTRI description:  ', '')
        sample['submitter_id'] = '{}-{}-octri'.format(line['ParticipantID'], c)
    if 'date_of_collection' in line:
        sample['submitter_id'] = line['sample_code']
    if 'submitter_id' not in sample:
        sample['submitter_id'] = '{}-sample'.format(participantid)
    return sample


def default_aliquot(sample_submitter_id, project_id):
    return { "type": "aliquot",
        "samples": {"submitter_id": sample_submitter_id},
        "submitter_id": '{}-aliquot'.format(sample_submitter_id),
        "project_id": project_id
    }


def default_treatment(treatment_submitter_id, diagnosis_submitter_id, treatment_type, project_id):
    return {
        'type': 'treatment',
        'project_id': project_id,
        'treatment_type': treatment_type,
        'diagnoses': {'submitter_id': diagnosis_submitter_id},
        'submitter_id': treatment_submitter_id
    }


def default_observation(case_submitter_id, project_id, date, observation_type, line):
    """Creates a minimal observation."""
    js = json.dumps(line, separators=(',',':'))
    check_sum = hashlib.sha256(js.encode('utf-8')).hexdigest()

    return {
        "type": "observation",
        "cases": {"submitter_id": case_submitter_id},
        "submitter_id": '{}-{}-{}'.format(case_submitter_id, observation_type, check_sum),
        "project_id": project_id,
        "date": date,
        "observation_type": observation_type
    }
