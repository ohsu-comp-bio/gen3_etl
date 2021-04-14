export EP='--endpoint https://htan-test.compbio.ohsu.edu'
export PP='--program ohsu --project simulatedhtan'
export CRED='--credentials_path config/credentials-htan-test.json'
export LOAD="python3 load/gen3_loader.py $EP $PP $CRED --path output/simulatedhtan"


$LOAD/project.ndjson
$LOAD/core_metadata_collection.ndjson
$LOAD/sequencing.ndjson
$LOAD/subject.ndjson
$LOAD/family.ndjson
$LOAD/sample.ndjson
$LOAD/reference_file.ndjson
$LOAD/discovery.ndjson


jq -c '.' project.json  > project.ndjson
jq -c '.[]' core_metadata_collection.json  > core_metadata_collection.ndjson
jq -c '.[]' sequencing.json  > sequencing.ndjson
jq -c '.[]' subject.json  > subject.ndjson
jq -c '.[]' family.json  > family.ndjson
jq -c '.[]' sample.json  > sample.ndjson
jq -c '.[]' reference_file.json  > reference_file.ndjson
jq -c '.[]' discovery.json  > discovery.ndjson


//TODO - doc below

jq -c '.[]' acknowledgement.json  > acknowledgement.ndjson
jq -c '.[]' aligned_reads_index.json  > aligned_reads_index.ndjson
jq -c '.[]' aliquot.json  > aliquot.ndjson
jq -c '.[]' case.json  > case.ndjson
jq -c '.[]' clinical_test.json  > clinical_test.ndjson
jq -c '.[]' core_metadata_collection.json  > core_metadata_collection.ndjson
jq -c '.[]' demographic.json  > demographic.ndjson
jq -c '.[]' diagnosis.json  > diagnosis.ndjson
jq -c '.[]' experiment.json  > experiment.ndjson
jq -c '.[]' experimental_metadata.json  > experimental_metadata.ndjson
jq -c '.[]' exposure.json  > exposure.ndjson
jq -c '.[]' family_history.json  > family_history.ndjson
jq -c '.[]' keyword.json  > keyword.ndjson
jq -c '.[]' publication.json  > publication.ndjson
jq -c '.[]' read_group.json  > read_group.ndjson
jq -c '.[]' read_group_qc.json  > read_group_qc.ndjson
jq -c '.[]' sample.json  > sample.ndjson
jq -c '.[]' slide.json  > slide.ndjson
jq -c '.[]' slide_count.json  > slide_count.ndjson
jq -c '.[]' slide_image.json  > slide_image.ndjson
jq -c '.[]' submitted_aligned_reads.json  > submitted_aligned_reads.ndjson
jq -c '.[]' submitted_copy_number.json  > submitted_copy_number.ndjson
jq -c '.[]' submitted_methylation.json  > submitted_methylation.ndjson
jq -c '.[]' submitted_somatic_mutation.json  > submitted_somatic_mutation.ndjson
jq -c '.[]' submitted_unaligned_reads.json  > submitted_unaligned_reads.ndjson
jq -c '.[]' treatment.json  > treatment.ndjson

$LOAD/experiment.ndjson
$LOAD/case.ndjson
$LOAD/sample.ndjson
$LOAD/aliquot.ndjson
$LOAD/acknowledgement.ndjson
$LOAD/diagnosis.ndjson
$LOAD/clinical_test.ndjson
$LOAD/core_metadata_collection.ndjson
$LOAD/experimental_metadata.ndjson
$LOAD/demographic.ndjson
$LOAD/submitted_copy_number.ndjson
$LOAD/read_group.ndjson
$LOAD/submitted_aligned_reads.ndjson
$LOAD/submitted_somatic_mutation.ndjson
$LOAD/slide.ndjson
$LOAD/keyword.ndjson
$LOAD/slide_count.ndjson
$LOAD/treatment.ndjson
$LOAD/exposure.ndjson
$LOAD/submitted_unaligned_reads.ndjson
$LOAD/read_group_qc.ndjson
$LOAD/slide_image.ndjson
$LOAD/publication.ndjson
$LOAD/aligned_reads_index.ndjson
$LOAD/family_history.ndjson
$LOAD/submitted_methylation.ndjson



from gen3.auth import Gen3Auth
from gen3.query import Gen3Query
from gen3.submission import Gen3Submission

endpoint = 'https://htan.compbio.ohsu.edu/'
cred = 'config/credentials-htan.json'

auth = Gen3Auth(endpoint, refresh_file=cred)

querier = Gen3Query(auth)
query_string = '{case{submitter_id}}'
print(querier.graphql_query(query_string))


submission_client = Gen3Submission(endpoint, auth)
program = 'simulatedhtan'
submission_client.create_program({'name': program, 'dbgap_accession_number': program,  'submitter_id': program, 'type': 'program'})



from gen3_etl.utils.gen3 import  submission_client

sc = submission_client(refresh_file=cred, endpoint=endpoint)


def submission_client(endpoint=None, refresh_file=None):
    """Create authorized client."""
    auth = Gen3Auth(endpoint, refresh_file=refresh_file)
    assert auth, 'should return an auth client'
    submission_client = Gen3Submission(endpoint, auth)
    print(f'opened client for {endpoint}')
    print(submission_client.get_programs())
    assert submission_client, 'should return a submission client'
    assert 'delete_program' in dir(submission_client), 'should have a delete_program method'
    assert 'create_program' in dir(submission_client), 'should have a create_program method'
    return submission_client



from gen3_etl.utils.gen3 import  submission_client
endpoint = 'https://htan.compbio.ohsu.edu/'
cred = 'config/credentials-htan.json'
sc = submission_client(refresh_file=cred, endpoint=endpoint)
