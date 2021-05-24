## gen3

export TEST_DATA_PATH="output/aced"
mkdir -p "$TEST_DATA_PATH"

export TEST_DATA_SCHEMA=https://htan-testing.s3-us-west-2.amazonaws.com/schema.json

docker run -it -v "${TEST_DATA_PATH}:/mnt/data" --rm --name=dsim --entrypoint=data-simulator quay.io/cdis/data-simulator:master simulate --url $TEST_DATA_SCHEMA --path /mnt/data --program jnkns --project jenkins --max_samples 10



# endpoint - where gen3 is hosted
export EP='--endpoint https://aced-gen3.ddns.net'
# program-project - the program & project we want to load, see https://aced-gen3.ddns.net/submission
export PP='--program aced --project simulated'
# credentials from https://aced-gen3.ddns.net/identity
export CRED='--credentials_path config/credentials.aced.json '
# Load program, sans file name
export LOAD="python3 load/gen3_loader.py $EP $PP $CRED --path output/aced"

# edit - change project code from jenkins to 'simulated'

# flatten

jq '.[]' -c aliquot.json > aliquot.ndjson 
jq '.[]' -c core_metadata_collection.json > core_metadata_collection.ndjson 
jq '.[]' -c discovery.json > discovery.ndjson 
jq '.[]' -c family.json > family.ndjson 
jq '.[]' -c file.json > file.ndjson 
jq '.[]' -c project.json > project.ndjson 
jq '.[]' -c reference_file.json > reference_file.ndjson 
jq '.[]' -c sample.json > sample.ndjson 
jq '.[]' -c sequencing.json > sequencing.ndjson 
jq '.[]' -c subject.json > subject.ndjson 


cat DataImportOrder.txt | xargs -I FN  $LOAD/"FN.ndjson"

# load the files in this order (schema dependencies)

$LOAD/project.json


# load ES

dc up -d spark-service tube-service
bash ./guppy_setup.sh
dc stop spark-service tube-service ; dc rm -f spark-service tube-service


dc up -d spark-service tube-service ; bash ./guppy_setup.sh ; dc stop spark-service tube-service ; dc rm -f spark-service tube-service