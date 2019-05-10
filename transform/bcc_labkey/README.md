To retrieve bcc data

```
ensure credentials

See https://github.com/LabKey/labkey-api-python#set-up-a-netrc-file

python3 transform/bcc_labkey/harvest.py

```



To transform

```
python3 transform/bcc_labkey/case.py
python3 transform/bcc_labkey/diagnosis.py
python3 transform/bcc_labkey/demographic.py
python3 transform/bcc_labkey/sample.py
python3 transform/bcc_labkey/aliquot.py

python3 transform/bcc_labkey/genetrails_variant.py


python3 load/gen3_dbloader.py $EP $PP --path output/bcc/case.json --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_participant.json  --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/diagnosis.json  --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_diagnosis.json  --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/demographic.json --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_demographic.json --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/sample.json --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_sample.json --delete_first true


```

To delete

```
# Quick delete, log into postgres
delete from node_acknowledgement          where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_alignedreadsindex        where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_aliquot                  where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_allele                   where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_case                     where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_clinicaltest             where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_compound                 where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_coremetadatacollection   where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_datarelease              where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_default                  where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_demographic              where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_diagnosis                where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_drugresponse             where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_experiment               where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_experimentalmetadata     where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_exposure                 where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_familyhistory            where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_gene                     where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_geneontologyterm         where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_genetrailsvariant        where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_keyword                  where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_phenotype                where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_project                  where _props->>'dbgap_accession_number' = 'bcc'  ;
delete from node_publication              where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_readgroup                where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_readgroupqc              where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_root                     where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_sample                   where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_slide                    where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_slidecount               where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_slideimage               where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_submittedalignedreads    where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_submittedcopynumber      where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_submittedfile            where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_submittedmethylation     where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_submittedsomaticmutation where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_submittedunalignedreads  where _props->>'project_id' = 'ohsu-bcc'  ;
delete from node_treatment                where _props->>'project_id' = 'ohsu-bcc'  ;

# from command line
python3 load/gen3_deleter.py $PP
```

To load

```

# export EP='--endpoint https://localhost'
# export EP='--endpoint https://gen3.compbio.ohsu.edu'
export PP='--program ohsu --project bcc'
python3 load/gen3_deleter.py $EP  $PP

python3 load/gen3_loader.py $EP $PP --path output/bcc/project.json
python3 load/gen3_loader.py $EP $PP --path output/bcc/experiment.json

python3 load/gen3_loader.py $EP $PP --path output/bcc/case.json
python3 load/gen3_loader.py $EP $PP --path output/bcc/bcc_participant.json

python3 load/gen3_loader.py $EP $PP --path output/bcc/demographic.json
python3 load/gen3_loader.py $EP $PP --path output/bcc/bcc_demographic.json

python3 load/gen3_loader.py $EP $PP --path output/bcc/diagnosis.json
python3 load/gen3_loader.py $EP $PP --path output/bcc/bcc_diagnosis.json

python3 load/gen3_loader.py $EP $PP --path output/bcc/sample.json
python3 load/gen3_loader.py $EP $PP --path output/bcc/aliquot.json

python3 load/gen3_loader.py $EP $PP --path output/bcc/allele.json

python3 load/gen3_loader.py $EP $PP --path output/bcc/read_group.json
python3 load/gen3_loader.py $EP $PP --path output/bcc/submitted_somatic_mutation.json
```
