from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from pprint import pprint
from string import Template
from attrdict import AttrDict
import os



DEFAULT_CREDENTIALS_PATH = os.path.join('.', 'credentials.json')
DEFAULT_HOST = 'gen3.compbio.ohsu.edu'
DEFAULT_ENDPOINT = f'https://{DEFAULT_HOST}'
PAGE_SIZE = 100

def submission_client(endpoint=DEFAULT_ENDPOINT, refresh_file=DEFAULT_CREDENTIALS_PATH):
    """Create authorized client."""
    auth = Gen3Auth(endpoint, refresh_file=refresh_file)
    assert auth, 'should return an auth client'
    submission_client = Gen3Submission(endpoint, auth)
    assert submission_client, 'should return a submission client'
    assert 'delete_program' in dir(submission_client), 'should have a delete_program method'
    assert 'create_program' in dir(submission_client), 'should have a create_program method'
    return submission_client


query_template = Template("""
{
  case(first:$first, offset:$offset) {
    project_id
    submitter_id
    demographics {
      gender
      year_of_death
    }
    diagnoses {
      submitter_id
      type
      primary_diagnosis
      days_to_birth
      ajcc_clinical_m
      ajcc_clinical_n
      ajcc_clinical_stage
      ajcc_clinical_t
    }
    observations {
      submitter_id
      type
      days_to_birth
      observation_type
      unit_of_measure
      measurement
      measurement2
      measurement3
      description
    }
    samples {
      submitter_id
      type
      days_to_birth
      sample_type
      tissue_type
    }
    treatments {
      submitter_id
      type
      days_to_birth
      treatment_type
      therapeutic_agents
    }
  }
}
""")



def fetch_events():
    client = submission_client()

    cases = []
    offset = 0

    page_size = PAGE_SIZE
    while True:
        query = query_template.substitute(first=page_size, offset=offset)
        event_data = client.query(query)
        offset += page_size
        case_page = event_data['data']['case']
        if len(case_page) == 0:
            break
        cases.extend(case_page)

    print(f'cases length: {len(cases)}')

    events = []
    for case in cases:
        case = AttrDict(case)
        case_info = AttrDict()
        case_info.case_submitter_id = int(case.submitter_id)
        case_info.gender = case.demographics[0].gender
        birth = AttrDict()
        birth.type = 'birth'
        birth.days_to_birth = 0
        birth.update(case_info)
        events.append(birth)
        if 'year_of_death' in case.demographics[0] and case.demographics[0].year_of_death:
            death = AttrDict()
            death.type = 'death'
            death.days_to_birth = case.demographics[0].year_of_death
            death.update(case_info)
            events.append(death)
        for e in case.diagnoses:
            e.update(case_info)
            events.append(e)
        for e in case.observations:
            e.update(case_info)
            e.type = e.observation_type
            events.append(e)
        for e in case.samples:
            e.update(case_info)
            events.append(e)
        for e in case.treatments:
            e.update(case_info)
            e.type = e.treatment_type
            events.append(e)

    print(f'events length: {len(events)}')
    return events
