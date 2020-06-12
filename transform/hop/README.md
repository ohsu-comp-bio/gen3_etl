# importing medable data

## auth

See [session based authentication](https://docs.medable.com/reference#section-session-based-authentication)

Create `.config.json` in this directory, using `.config.json.sample` as a guide.

## transform

```
python3 transform/hop/case.py
python3 transform/hop/survey.py
python3 transform/hop/file.py
```

## connect to gen3
```
export PP='--program ohsu --project hop'
export EP='--endpoint https://localhost'
export psql='docker-compose exec -T --user postgres postgres psql -d metadata_db'

# also, check config/credentials.json
```


## create project & experiment
```
python3 load/gen3_loader.py $EP $PP --path output/hop/project.json
python3 load/gen3_dbloader.py $EP $PP --path output/hop/experiment.json
```

## create load files
```
python3 load/gen3_dbloader.py $EP $PP --path output/hop/project.json
python3 load/gen3_dbloader.py $EP $PP --path output/hop/case.json --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/hop/survey.json  --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/hop/submitted_file.json --delete_first true
```

## todo
```
# need to manually change id of experiment
vi output/hop/edge_casememberofexperiment.tsv
```

## load

```
$psql -c "delete from node_submittedfile where _props->>'project_id' = 'ohsu-hop'  ;"

$psql -c "delete from node_hopsurvey where _props->>'project_id' = 'ohsu-hop'  ;"

$psql -c "delete from node_case where _props->>'project_id' = 'ohsu-hop'  ;"

$psql -c "delete from node_experiment where _props->>'project_id' = 'ohsu-hop'  ;"

cat ~/gen3_etl/output/hop/node_experiment.tsv | $psql -c "copy node_experiment(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat ~/gen3_etl/output/hop/edge_experimentperformedforproject.tsv | $psql -c "copy edge_experimentperformedforproject(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat ~/gen3_etl/output/hop/node_case.tsv | $psql -c "copy node_case(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat ~/gen3_etl/output/hop/edge_casememberofexperiment.tsv | $psql -c "copy edge_casememberofexperiment(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat ~/gen3_etl/output/hop/node_hopsurvey.tsv | $psql -c "copy node_hopsurvey(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat ~/gen3_etl/output/hop/edge_hopsurveydescribescase.tsv | $psql -c "copy edge_hopsurveydescribescase(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat ~/gen3_etl/output/hop/node_submittedfile.tsv | $psql -c "copy node_submittedfile(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat ~/gen3_etl/output/hop/edge_submittedfiledatafromaliquot.tsv | $psql -c "copy edge_submittedfiledatafromaliquot(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"


cat ~/gen3_etl/output/hop/edge_submittedfiledataforcase.tsv | $psql -c "copy edge_submittedfiledataforcase(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

```

## verify query

```
$psql -c "select count(*) from node_case where _props->>'project_id' = 'ohsu-hop';"

count
-------
 2818


$psql -c "select count(*)  from node_hopsurvey where _props->>'project_id' = 'ohsu-hop'  ;"
count
--------
142387

$psql -c "select count(*)  from node_submittedfile where _props->>'project_id' = 'ohsu-hop'  ;"

count
-------
 2674

```


## delete

```
$psql -c "delete from node_submittedfile where _props->>'project_id' = 'ohsu-hop'  ;"
$psql -c "delete from node_hopsurvey where _props->>'project_id' = 'ohsu-hop'  ;"
$psql -c "delete from node_case where _props->>'project_id' = 'ohsu-hop'  ;"
```
