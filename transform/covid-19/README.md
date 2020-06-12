
export EP='--endpoint https://gen3.compbio.ohsu.edu'
export PP='--program ohsu --project covid-19'
python3 load/gen3_loader.py $EP $PP --path output/covid-19/project.json
python3 load/gen3_loader.py $EP $PP --path output/covid-19/location.json
python3 load/gen3_loader.py $EP $PP --path output/covid-19/study.json
python3 load/gen3_loader.py $EP $PP --path output/covid-19/subject.json
python3 load/gen3_loader.py $EP $PP --path output/covid-19/sample.json

python3 load/gen3_loader.py $EP $PP --path output/covid-19/core_metadata_collection.json

gen3-client upload --upload-path=covid-19.txt

#capture file guid b6dddab8-e050-463a-98e6-4957dd5cdea9



# delete for project
```
\c metadata_db ;
# Quick delete, log into postgres
delete from node_study                    where _props->>'project_id' = 'ohsu-covid-19'  ;
delete from node_summarylocation          where _props->>'project_id' = 'ohsu-covid-19'  ;
delete from transaction_documents where transaction_id in (select id from transaction_logs where project = 'covid-19');
delete from transaction_snapshots where transaction_id in (select id from transaction_logs where project = 'covid-19');
delete from transaction_logs              where project = 'covid-19' ;
delete from node_project                  where _props->>'code' = 'covid-19'  ;
```


# show all tables & their counts:
```
SELECT schemaname,relname,n_live_tup 
  FROM pg_stat_user_tables 
  ORDER BY n_live_tup DESC;
```

# graphql

```
{
  project(name: "covid-19") {code summary_locations { submitter_id } studies {data_description}}
}
```

# export PP='--program ohsu --project covid-19'

```
from gen3.file import Gen3File
from gen3.auth import Gen3Auth

endpoint = 'https://gen3.compbio.ohsu.edu'
auth = Gen3Auth(endpoint, refresh_file="./config/credentials.json")
gen3_file_client = Gen3File(endpoint, auth)
print(gen3_file_client.get_presigned_url('b6dddab8-e050-463a-98e6-4957dd5cdea9'))



```

