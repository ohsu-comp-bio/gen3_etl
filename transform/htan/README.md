export EP='--endpoint https://htan-test.compbio.ohsu.edu'
export PP='--program ohsu --project htan'
export CRED='--credentials_path config/credentials-htan-test.json '
export LOAD="python3 load/gen3_loader.py $EP $PP $CRED --path output/htan"


$LOAD/project.ndjson
$LOAD/subject.ndjson
$LOAD/sample.ndjson
$LOAD/aliquot.ndjson

# $LOAD/sequencing.ndjson

$LOAD/file.ndjson



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

![image](https://user-images.githubusercontent.com/47808/114777046-0bf83280-9d28-11eb-8d7c-0c599004ab1b.png)



delete from node_sequencing where _props->>'project_id' = 'ohsu-htan'  ;
delete from node_file where _props->>'project_id' = 'ohsu-htan'  ;
delete from node_aliquot where _props->>'project_id' = 'ohsu-htan'  ;
delete from node_sample where _props->>'project_id' = 'ohsu-htan'  ;