
# H2 import data into gen3

## First run transform

```
python3 transform/htan/transform.py
```

This will produce files in output/htan/

```
$ ls -l output/htan/
total 67976
-rw-r--r--  1 walsbr  OHSUM01\Domain Users     23346 Apr 29 15:11 aliquot.ndjson
-rw-r--r--  1 walsbr  OHSUM01\Domain Users       161 Apr 29 15:11 experiment.ndjson
-rw-r--r--  1 walsbr  OHSUM01\Domain Users  18123213 Apr 29 15:11 file.ndjson
-rw-r--r--  1 walsbr  OHSUM01\Domain Users       132 Apr 29 15:11 project.ndjson
-rw-r--r--  1 walsbr  OHSUM01\Domain Users     22979 Apr 29 15:11 sample.ndjson
-rw-r--r--  1 walsbr  OHSUM01\Domain Users  15378784 Mar 22 12:00 sequencing.ndjson
-rw-r--r--  1 walsbr  OHSUM01\Domain Users      4197 Apr 29 15:11 subject.ndjson
```

## Load these files into Gen3

```
# endpoint - where gen3 is hosted
export EP='--endpoint https://htan-test.compbio.ohsu.edu'
# program-project - the program & project we want to load, see https://htan-test.compbio.ohsu.edu/submission
export PP='--program ohsu --project htan'
# credentials from https://htan-test.compbio.ohsu.edu/identity
export CRED='--credentials_path config/credentials-htan-test.json '
# Load program, sans file name
export LOAD="python3 load/gen3_loader.py $EP $PP $CRED --path output/htan"

# load the files in this order (schema dependencies)

$LOAD/project.ndjson
$LOAD/subject.ndjson
$LOAD/sample.ndjson
$LOAD/aliquot.ndjson
$LOAD/file.ndjson

```

# gen3 server instance

```
# remember to add `10.96.11.189 htan-test` to your /etc/hosts
ssh ubuntu@htan-test

# instance is hosted at `test/compose-services/`
alias dc='docker-compose'
dc ps
# should show ...

      Name                     Command                   State                            Ports
--------------------------------------------------------------------------------------------------------------------
arborist-service    bash /go/src/github.com/uc ...   Up (healthy)
esproxy-service     /bin/bash -c echo -e 'clus ...   Up (unhealthy)   0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp
fence-service       sh /entrypoint.sh bash /va ...   Up (healthy)     443/tcp, 80/tcp
guppy-service       /usr/bin/wait_for_esproxy. ...   Up               3000/tcp, 80/tcp
indexd-service      sh /entrypoint.sh bash ind ...   Up (healthy)     443/tcp, 80/tcp
kibana-service      /usr/local/bin/kibana-docker     Up               0.0.0.0:5601->5601/tcp
metadata-service    sh -c /env/bin/alembic upg ...   Up
peregrine-service   sh /entrypoint.sh /bin/sh  ...   Up (healthy)     443/tcp, 80/tcp
pidgin-service      sh /entrypoint.sh /bin/sh  ...   Up (healthy)     443/tcp, 80/tcp
portal-service      bash /var/www/data-portal/ ...   Up (healthy)
postgres            docker-entrypoint.sh postg ...   Up (healthy)     0.0.0.0:5432->5432/tcp
revproxy-service    /bin/sh -c nginx -g 'daemo ...   Up (healthy)     0.0.0.0:443->443/tcp, 0.0.0.0:80->80/tcp
sheepdog-service    sh /entrypoint.sh bash /sh ...   Up (healthy)     443/tcp, 80/tcp
spark-service       bash -c python run_config. ...   Exit 137
tube-service        bash -c while true; do sle ...   Exit 137

```
See https://github.com/uc-cdis/compose-services for documentation
I've started an overview doc here https://docs.google.com/document/d/16PkPBqE3gzzHmUTWtSG1P5mRE-08jE3yMXJ3Mf2D4a0/edit



# Issues:

Some paths don't have bem ids.
e.g
```
warn:file_missing_bems /./HTA9_2/HMS-SORGER/t-CycIF_Tumor_Panel/CSB08/registration/CSB08.antibody.metadata.csv
```

Some bems ids are missing from spreadsheet?
Are we identifying all biopsies?  Should all files have a biopsy?
Are we identifying pipeline_stages correctly?
We have a fake md5sum for now, should we harvest w/ next directory listing?
Have we identified data_category correctly?


# etc

quick delete, log into the postgres image and delete from database

```
delete from node_sequencing where _props->>'project_id' = 'ohsu-htan'  ;
delete from node_file where _props->>'project_id' = 'ohsu-htan'  ;
delete from node_aliquot where _props->>'project_id' = 'ohsu-htan'  ;
delete from node_sample where _props->>'project_id' = 'ohsu-htan'  ;
```



# Sample queries
# Give me all level 1 files for case 2 (aka "HTA9_1").
```
{
	subject(submitter_id:"HTA9_1") {
    submitter_id
    
    samples  {
      submitter_id
      biopsy
      institution
      files(data_processing_stage:"level1") {
				data_processing_pipeline
        data_processing_stage
        submitter_id
      }      
    }    
  }
}
```
![image](https://user-images.githubusercontent.com/47808/114775747-845df400-9d26-11eb-9b59-10ae953a854e.png)


# Do we have data files for case 3 (aka HTA9_3) biopsy 1 from cmIF?
```
{
	subject(submitter_id:"HTA9_3") {
    submitter_id
    
    samples(biopsy:"1")  {
      submitter_id
      biopsy
      institution
      files(data_processing_pipeline:"cmIF") {
				data_processing_pipeline
        data_processing_stage
        submitter_id
      }      
    }    
  }
}
```
![image](https://user-images.githubusercontent.com/47808/114776219-16fe9300-9d27-11eb-84bb-1057ba096ffc.png)



# Give me all level 2 files from HMS-Sorger.

Note:  No files for that `institution`

```
{    
    sample(institution:"HMS, Sorger")  {
      submitter_id
      biopsy
      institution
      files(data_processing_stage:"level2") {
				data_processing_pipeline
        data_processing_stage
        submitter_id
      }      
    }    
}
```

![image](https://user-images.githubusercontent.com/47808/114777046-0bf83280-9d28-11eb-8d7c-0c599004ab1b.png)


# Give me all of the existing case numbers

```
{    
    subject {
			submitter_id
    }
}
```