import json
from dateutil.parser import parse
from pprint import pprint

###

def convert(string, fuzzy=False, debug=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        return parse(string, fuzzy=fuzzy)
    except:
        if debug:
          print('could not parse', string)
        return string



def get_diagnosis(case):
    """Returns site and diagnosis."""
    site = 'unknown'
    diagnosis = 'unknown'
    if 'diagnoses' in case:
        for d in case['diagnoses']:
            _site = d.get('site', None)
            if _site:
                site = _site
            _diagnosis = d.get('diagnosis', None)
            if _diagnosis:
                diagnosis = _diagnosis
    return site, diagnosis

def make_case(case, project_id='ohsu-bcc'):
    """Returns gen3 case."""
    assert case.get('submitter_id', None), case
    site, diagnosis = get_diagnosis(case)
    return {
      "type": "case",
      "experiments": {"submitter_id": project_id},
      "submitter_id": case['submitter_id'],
      "disease_type": diagnosis,
      "consent_codes": [],
      "primary_site": site,
      "project_id": project_id,
      # "source": case['source']
    }

def age_at_diagnosis(case, diagnosis):
    if 'dateofbirth' not in case:
        return None
    if not case['dateofbirth']:
        return None
    try:
        return int((convert(diagnosis['timestamp']) - convert(case['dateofbirth'])).days/365)
    except Exception as e:
        pprint(diagnosis)
        raise e

def days_to_birth(case, event):
    """Time interval from a person's date of birth to the
       date of event, represented as a calculated negative number of day."""
    if 'dateofbirth' not in case or case['dateofbirth'] is None:
        return None
    try:
        return int((convert(event['timestamp']) - convert(case['dateofbirth'])).days * -1)
    except Exception as e:
        pprint(event)
        raise e


def morphology(case, diagnosis):
    if 'stage' not in diagnosis:
        return 'Unknown'
    if isinstance(diagnosis['stage'], str):
        print(diagnosis)
        exit(0)
    if diagnosis['stage']['source']:
        return diagnosis['stage']['source']
    return 'Unknown'

def primary_diagnosis(case, diagnosis):
    primary_diagnosis = diagnosis.get('diagnosis', None)
    if not primary_diagnosis:
        return 'Unknown'
    return primary_diagnosis

def site_of_resection_or_biopsy(case, diagnosis):
    site = diagnosis.get('site', None)
    if site:
        return site
    return 'Unknown'

def tissue_or_organ_of_origin(case, diagnosis):
    return site_of_resection_or_biopsy('site', diagnosis)

def tumor_stage(case, diagnosis):
    if 'stage' not in diagnosis:
        return 'Unknown'
    if diagnosis['stage']['stage']:
        return diagnosis['stage']['stage']
    return 'Unknown'

def tumor_grade(case, diagnosis):
    if 'stage' not in diagnosis:
        return 'Unknown'
    if diagnosis['stage']['stage_rating']:
        return diagnosis['stage']['stage_rating']
    return 'Unknown'

def vital_status(case, diagnosis):
    if 'dateofdeath' not in case:
        return 'unknown'
    if case['dateofdeath']:
        return 'dead'
    return 'alive'

def is_category(diagnosis, category):
  # 'clinical': [compile('^c')],
  # 'pathologic': [compile('^p')],
  # 'recurrence': [compile('^r')],
  # 'autopsy': [compile('^a')],
    if 'stage' not in diagnosis:
        return False
    # If no designation before TNM, c is presumed
    if diagnosis['stage']['category'] is None and category == 'clinical':
        return True
    if diagnosis['stage']['category'] == category:
        return True
    return False


def ajcc_clinical_m(case, diagnosis):
    if not is_category(diagnosis, 'clinical'):
        return 'Unknown'
    if not diagnosis['stage']['metastases']:
        return 'Unknown'
    return f"M{diagnosis['stage']['metastases']}"

def ajcc_clinical_n(case, diagnosis):
    if not is_category(diagnosis, 'clinical'):
        return 'Unknown'
    if not diagnosis['stage']['node']:
        return 'Unknown'
    return f"N{diagnosis['stage']['node']}"

def int_to_roman(input):
    """ Convert an integer to a Roman numeral. """

    if not isinstance(input, type(1)):
        raise TypeError("expected integer, got %s" % type(input))
    if input == 0:
        return '0'
    if not 0 < input < 4000:
        raise ValueError(f"Argument must be between 1 and 3999 {input}")
    ints = (1000, 900,  500, 400, 100,  90, 50,  40, 10,  9,   5,  4,   1)
    nums = ('M',  'CM', 'D', 'CD','C', 'XC','L','XL','X','IX','V','IV','I')
    result = []
    for i in range(len(ints)):
        count = int(input / ints[i])
        result.append(nums[i] * count)
        input -= ints[i] * count
    return ''.join(result)


def ajcc_clinical_stage(case, diagnosis):
    if not is_category(diagnosis, 'clinical'):
        return 'Unknown'
    if not diagnosis['stage']['stage']:
        return 'Unknown'
    stage_rating = ''
    if diagnosis['stage']['stage_rating']:
        stage_rating = diagnosis['stage']['stage_rating']
    return f"Stage {int_to_roman(int(diagnosis['stage']['stage']))}{stage_rating}"

def ajcc_clinical_t(case, diagnosis):
    if not is_category(diagnosis, 'clinical'):
        return 'Unknown'
    if not diagnosis['stage']['tumor']:
        return 'Unknown'
    return f"T{diagnosis['stage']['tumor']}"

def ajcc_pathologic_m(case, diagnosis):
    if not is_category(diagnosis, 'pathologic'):
        return 'Unknown'
    if not diagnosis['stage']['metastases']:
        return 'Unknown'
    return f"M{diagnosis['stage']['metastases']}"

def ajcc_pathologic_n(case, diagnosis):
    if not is_category(diagnosis, 'pathologic'):
        return 'Unknown'
    if not diagnosis['stage']['node']:
        return 'Unknown'
    return f"N{diagnosis['stage']['node']}"

def ajcc_pathologic_stage(case, diagnosis):
    if not is_category(diagnosis, 'pathologic'):
        return 'Unknown'
    if not diagnosis['stage']['stage']:
        return 'Unknown'
    stage_rating = ''
    if diagnosis['stage']['stage_rating']:
        stage_rating = diagnosis['stage']['stage_rating']
    return f"Stage {int_to_roman(int(diagnosis['stage']['stage']))}{stage_rating}"


def ajcc_pathologic_t(case, diagnosis):
    if not is_category(diagnosis, 'pathologic'):
        return 'Unknown'
    if not diagnosis['stage']['tumor']:
        return 'Unknown'
    return f"T{diagnosis['stage']['tumor']}"

def make_diagnosis(case, diagnosis, project_id='ohsu-bcc'):
    gen3_diagnosis = {
      "type": "diagnosis",
      "age_at_diagnosis": age_at_diagnosis(case, diagnosis),
      "classification_of_tumor": 'Unknown',
      "days_to_birth": diagnosis['days_to_birth'],
      "days_to_last_follow_up": None,
      "days_to_last_known_disease_status": None,
      "days_to_recurrence": None,
      "last_known_disease_status": 'Unknown tumor status',
      "morphology": morphology(case, diagnosis),
      "primary_diagnosis": primary_diagnosis(case, diagnosis),
      "progression_or_recurrence": 'unknown',
      "site_of_resection_or_biopsy": site_of_resection_or_biopsy(case, diagnosis),
      "submitter_id": diagnosis['submitter_id'],
      "tissue_or_organ_of_origin": tissue_or_organ_of_origin(case, diagnosis),
      "tumor_grade": tumor_grade(case, diagnosis),
      "tumor_stage": tumor_stage(case, diagnosis),
      "vital_status": vital_status(case, diagnosis),
      "ajcc_clinical_m": ajcc_clinical_m(case, diagnosis),
      "ajcc_clinical_n": ajcc_clinical_n(case, diagnosis),
      "ajcc_clinical_stage": ajcc_clinical_stage(case, diagnosis),
      "ajcc_clinical_t": ajcc_clinical_t(case, diagnosis),
      "ajcc_pathologic_m": ajcc_pathologic_m(case, diagnosis),
      "ajcc_pathologic_n": ajcc_pathologic_n(case, diagnosis),
      "ajcc_pathologic_stage": ajcc_pathologic_stage(case, diagnosis),
      "ajcc_pathologic_t": ajcc_pathologic_t(case, diagnosis),
      "cases": {
        "submitter_id": case['submitter_id']
      },
      # "source": diagnosis['source'],
    }
    if gen3_diagnosis['ajcc_pathologic_stage'] == 'Unknown':
        del gen3_diagnosis['ajcc_pathologic_stage']

    # _days_to_birth = days_to_birth(case, diagnosis)
    # if _days_to_birth:
    #   gen3_diagnosis["days_to_birth"] = _days_to_birth

    return gen3_diagnosis

def gender(case):
    if 'gender' not in case:
        return None
    return case['gender'].lower()

# def year_of_birth(case):
#     if 'dateofbirth' not in case:
#         return None
#     return convert(case['dateofbirth']).year
#
def year_of_death(case):
    if 'dateofdeath' not in case or not case['dateofdeath']:
        return None
    return int((convert(case['dateofdeath']) - convert(case['dateofbirth'])).days * -1)

def make_demographic(case, project_id='ohsu-bcc'):
    demographic = {
      "gender": gender(case),
      "type": "demographic",
      # "year_of_birth": year_of_birth(case),
      "cases": {
        "submitter_id": case['submitter_id']
      },
      "submitter_id": f"{case['submitter_id']}/demographic",
      "project_id": project_id,
      # "source": case['source'],
    }
    _year_of_death = year_of_death(case)
    if _year_of_death:
        demographic["year_of_death"] = year_of_death(case)
    return demographic

def tissue_type(sample):
    # tissue_type = sample['origin']
    # if not tissue_type:
    #     tissue_type = 'Unknown'
    # return tissue_type
    return 'Tumor'

def sample_type(sample):
    sample_type = sample['sample_type']
    if not sample_type:
        sample_type = 'Unknown'
    if sample_type in ['Knight Diagnostic Labs']:
        sample_type = 'Unknown'

    return sample_type

def make_sample(case, sample):
    gen3_sample = {
      "submitter_id": sample['submitter_id'],
      "cases": {
        "submitter_id": case['submitter_id']
      },
      "type": sample['type'],
      # "source": sample['source'],
      # "diagnoses": {
      #   "submitter_id": null
      # },
      # "sample_type_id": null,
      # "time_between_excision_and_freezing": null,
      # "oct_embedded": null,
      # "tumor_code_id": null,
      # "intermediate_dimension": null,
      # "sample_volume": null,
      # "time_between_clamping_and_freezing": null,
      # "tumor_descriptor": null,
      "sample_type": sample_type(sample),
      # "biospecimen_anatomic_site": null,
      # "diagnosis_pathologically_confirmed": null,
      # "project_id": null,
      # "current_weight": null,
      # "composition": null,
      # "is_ffpe": null,
      # "shortest_dimension": null,
      # "method_of_sample_procurement": null,
      # "tumor_code": null,
      "tissue_type": tissue_type(sample),
      # "days_to_sample_procurement": null,
      # "freezing_method": null,
      # "preservation_method": null,
      # "days_to_collection": null,
      # "initial_weight": null,
      # "longest_dimension": null
    }
    _days_to_birth = days_to_birth(case, sample)
    if _days_to_birth:
      gen3_sample["days_to_birth"] = _days_to_birth
    return gen3_sample


def make_read_group(case, sample, aliquot, read_group):
    return {
      "submitter_id": f"{aliquot['submitter_id']}/read_group/{read_group['timestamp']}",
      "aliquots": {
        "submitter_id": aliquot['submitter_id']
      },
      "type": "read_group",
      # "source": read_group['source'],
      # "library_name": null,
      # "is_paired_end": null,
      # "size_selection_range": null,
      # "adapter_sequence": null,
      # "library_strand": null,
      # "library_preparation_kit_name": null,
      # "spike_ins_fasta": null,
      # "target_capture_kit_name": null,
      # "base_caller_name": null,
      # "library_preparation_kit_version": null,
      # "spike_ins_concentration": null,
      # "target_capture_kit_vendor": null,
      # "read_length": null,
      # "sequencing_date": null,
      # "to_trim_adapter_sequence": null,
      # "RIN": null,
      # "platform": null,
      # "barcoding_applied": null,
      # "library_selection": null,
      # "library_strategy": null,
      # "library_preparation_kit_catalog_number": null,
      # "base_caller_version": null,
      # "target_capture_kit_target_region": null,
      # "target_capture_kit_version": null,
      # "read_group_name": null,
      # "library_preparation_kit_vendor": null,
      # "project_id": null,
      # "target_capture_kit_catalog_number": null,
      # "instrument_model": null,
      # "includes_spike_ins": null,
      # "experiment_name": null,
      # "flow_cell_barcode": null,
      # "sequencing_center": null,
      # "adapter_name": null
    }

def make_aliquot(case, sample, aliquot):
    return {
      "type": "aliquot",
      "samples": {
        "submitter_id": sample['submitter_id']
      },
      "submitter_id": aliquot['submitter_id'],
      # "source": sample['source'],
      # "aliquot_quantity": null,
      # "aliquot_volume": null,
      # "analyte_type_id": null,
      # "analyte_type": null,
      # "amount": null,
      # "concentration": null,
      # "project_id": null,
      # "source_center": null
    }

def make_allele(case, sample, aliquot, read_group, allele):
    # TODO
    return allele


def make_treatment(case, diagnosis, treatment):
        # treatment.source
        # "Radiotherapy"
        # "treatment_chemotherapy_manually_entered"
        # "treatment_chemotherapy_ohsu"
        # "treatment_chemotherapy_regimen"

      # {
      #   "source": "treatment_chemotherapy_ohsu",
      #   "unit_of_measure": "Milligram",
      #   "treatment_description": "INV SUGAMMADEX 200 MG/2 ML INTRAVENOUS SOLUTION (IRB 16351)",
      #   "title": "6766__04/19/18__SUGAMMADEX",
      #   "treatment_agent": "SUGAMMADEX",
      #   "delivery_method": "CHEMOEMBOLIZATION",
      #   "total_dose": 200,
      #   "timestamp": "2018-04-19T16:01:00",
      #   "submitter_id": "6766/treatment_chemotherapy_ohsu/2018-04-19 16:01:00/c8f7102a-ba00-11e9-92ec-005056be6474",
      #   "type": "treatment"
      # }
    gen3_treatment = {
      "type": "treatment",
      "submitter_id": treatment['submitter_id'] ,
      # "source": treatment['source'],
      # "days_to_treatment_start": null,
      # "treatment_type": null,
      # "days_to_treatment": null,
      # "therapeutic_agents": null,
      # "treatment_intent_type": null,
      # "treatment_anatomic_site": null,
      # "project_id": null,
      # "treatment_outcome": null,
      # "days_to_treatment_end": null,
      # "treatment_or_therapy": null
    }

    _treatment_type = None
    if 'chemotherapy' in treatment['source'].lower():
        _treatment_type = 'Chemotherapy'
    if 'radiotherapy' in treatment['source'].lower():
        _treatment_type = 'Radiation Therapy'
    if 'surgery' in treatment['source'].lower():
        _treatment_type = 'Surgery'
    if _treatment_type:
        gen3_treatment["treatment_type"] = _treatment_type

    if 'therapeutic_agent' in treatment and treatment['therapeutic_agent']:
        gen3_treatment["therapeutic_agents"] = treatment['therapeutic_agent']


    if diagnosis:
        gen3_treatment["diagnoses"] = {"submitter_id": diagnosis['submitter_id']}
    if case:
        gen3_treatment["cases"] = {"submitter_id": case['submitter_id']}
        _days_to_birth = days_to_birth(case, treatment)
        if _days_to_birth:
          gen3_treatment["days_to_birth"] = _days_to_birth

    return gen3_treatment

def measurement(observation):
    keys = ['height', 'weight', 'result_number_value', 'sizeaxis1', 'biomarker_level']
    value = None
    for k in keys:
        value = observation.get(k, None)
        if value:
            break
    return value

def measurement2(observation):
    keys = ['sizeaxis2']
    value = None
    for k in keys:
        value = observation.get(k, None)
        if value:
            break
    return value


def measurement3(observation):
    keys = ['sizeaxis3']
    value = None
    for k in keys:
        value = observation.get(k, None)
        if value:
            break
    return value

def measurement_description(observation):
    keys = ['glycemic_assay', 'notes', 'image_site_description', 'comments']
    values = []
    for k in keys:
        value = observation.get(k, None)
        if value:
            values.append(value)
    if len(values) > 0:
        return ' '.join(values)
    return None

def unit_of_measure(observation):
    return observation.get('unit_of_measure', observation.get('glycemic_unit_of_measure', None))

def make_observation(case, observation):
    gen3_observation = {
      "days_to_birth": days_to_birth(case, observation),
      "type": "observation",
      "submitter_id": observation['submitter_id'],
      "cases": {
        "submitter_id": case['submitter_id']
      },
      "observation_type": observation['source']
    }

    _measurement = measurement(observation)
    if _measurement:
      gen3_observation["measurement"] = _measurement

    _measurement = measurement2(observation)
    if _measurement:
      gen3_observation["measurement2"] = _measurement

    _measurement = measurement3(observation)
    if _measurement:
      gen3_observation["measurement3"] = _measurement

    _description = measurement_description(observation)
    if _description:
      gen3_observation["description"] = _description
    _unit_of_measure = unit_of_measure(observation)
    if _unit_of_measure:
      gen3_observation["unit_of_measure"] = _unit_of_measure
    if 'lesion_key' in observation:
      gen3_observation["observation_key"] = observation["lesion_key"]

    return gen3_observation




cases = open('output/bcc/cases.json', 'w')
diagnoses = open('output/bcc/diagnoses.json', 'w')
demographics = open('output/bcc/demographics.json', 'w')
treatments = open('output/bcc/treatments.json', 'w')
samples = open('output/bcc/samples.json', 'w')
aliquots = open('output/bcc/aliquots.json', 'w')
read_groups = open('output/bcc/read_groups.json', 'w')
alleles = open('output/bcc/alleles.json', 'w')
observations = open('output/bcc/observations.json', 'w')

with open('output/bcc/graph.json') as input:
    for line in input:
        line = json.loads(line)
        json.dump(make_case(line), cases)
        cases.write('\n')
        json.dump(make_demographic(line), demographics)
        demographics.write('\n')
        if 'treatments' in line:
            for treatment in line['treatments']:
                json.dump(make_treatment(line, None, treatment), treatments)
                treatments.write('\n')
        if 'diagnoses' in line:
            for diagnosis in line['diagnoses']:
                json.dump(make_diagnosis(line, diagnosis), diagnoses)
                diagnoses.write('\n')
        if 'observations' in line:
            for observation in line['observations']:
                json.dump(make_observation(line, observation), observations)
                observations.write('\n')
        if 'samples' in line:
            for sample in line['samples']:
                json.dump(make_sample(line, sample), samples)
                samples.write('\n')
                for aliquot in sample['aliquots']:
                    json.dump(make_aliquot(line, sample, aliquot), aliquots)
                    aliquots.write('\n')
                    for read_group in aliquot['read_groups']:
                        json.dump(make_read_group(line, sample, aliquot, read_group), read_groups)
                        read_groups.write('\n')
                        assert 'alleles' in read_group, read_group
                        for allele in read_group['alleles']:
                            json.dump(make_allele(line, sample, aliquot, read_group, allele), alleles)
                            alleles.write('\n')

cases.close()
demographics.close()
diagnoses.close()
samples.close()
aliquots.close()
read_groups.close()
alleles.close()
treatments.close()
observations.close()
# set([''.join(sample.keys()) for sample in original_samples[:10]])
