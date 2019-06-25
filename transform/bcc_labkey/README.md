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
python3 transform/bcc_labkey/treatment.py


python3 load/gen3_dbloader.py $EP $PP --path output/bcc/case.json --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_participant.json  --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/diagnosis.json  --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_diagnosis.json  --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/demographic.json --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_demographic.json --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/sample.json --delete_first true
python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_sample.json --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/aliquot.json --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/genetrails_variant.json --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/treatment.json --delete_first true

python3 load/gen3_dbloader.py $EP $PP --path output/bcc/bcc_chemotherapy.json --delete_first true

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




```
Cases:
  cat source/bcc/vDemoGraphicsForPlot.json  source/bcc/vcancerparticipantoverview.json  | jq .participantid | sort -u | wc -l
  >>> 1720

Diagnosis:
  cat source/bcc/voncologdiagnosis.json | jq .participantid | sort -u | wc -l
  >>> 766

Demographics:
  cat source/bcc/vDemoGraphicsForPlot.json | jq .participantid | sort -u | wc -l
  >>> 1720

Observations:
  cat source/bcc/lesion_size.json | jq .ParticipantID  | sort -u | wc -l
  >>> 142
  cat source/bcc/vWeightMonthly.json  | jq .participantid | sort -u | wc -l  
  >>> 1663
  cat source/bcc/weight_ohsu.json | jq .ParticipantID  | sort -u | wc -l
  >>> 1663
  cat source/bcc/height_ohsu.json  | jq .ParticipantID  | sort -u | wc -l
  >>> 1655  # TODO  


Samples:
  cat source/bcc/vbiolibraryspecimens.json |  jq .participantid  | sort -u | wc -l
  >>> 1282
  cat source/bcc/Samples.json |  jq .ParticipantID  | sort -u | wc -l
  >>> 329  
  cat source/bcc/sample_genetrails_assay.json |  jq .ParticipantID  | sort -u | wc -l
  >>> 85
  cat source/bcc/sample.json  |  jq .ParticipantID  | sort -u | wc -l
  >>> 1515    

  cat source/bcc/vbiolibraryspecimens.json |  jq .participantid  | sort -u > /tmp/ttt
  cat source/bcc/Samples.json |  jq .ParticipantID  | sort -u >> /tmp/ttt
  cat source/bcc/sample_genetrails_assay.json |  jq .ParticipantID  | sort -u >> /tmp/ttt
  cat source/bcc/sample.json  |  jq .ParticipantID  | sort -u >> /tmp/ttt
  cat /tmp/ttt | sort -u | wc -l
  >>> 1611

  cat source/bcc/vbiolibraryspecimens.json source/bcc/Samples.json source/bcc/sample_genetrails_assay.json  source/bcc/sample.json | wc -l
  >>> 43642

Aliquots:
  cat source/bcc/sample.json  |  jq .ParticipantID  | sort -u | wc -l
  >>> 1515    
  cat source/bcc/sample.json  | wc -l
  >>> 4208

Treatments:
  cat source/bcc/treatment_chemotherapy_ohsu.json |  jq .ParticipantID  | sort -u | wc -l
  >>> 292
  cat source/bcc/treatment_chemotherapy_manually_entered.json |  jq .ParticipantID  | sort -u | wc -l
  >>> 113
  cat source/bcc/vResectionDate.json |  jq .participantid  | sort -u | wc -l
  >>> 527
  cat source/bcc/voncologsurgery.json |  jq .participantid  | sort -u | wc -l
  >>> 764
  cat source/bcc/Radiotherapy.json |  jq .ParticipantID  | sort -u | wc -l
  >>> 85

  cat source/bcc/treatment_chemotherapy_ohsu.json \
    source/bcc/treatment_chemotherapy_manually_entered.json \
    source/bcc/vResectionDate.json \
    source/bcc/voncologsurgery.json \
    source/bcc/Radiotherapy.json \
    | wc -l
  >>> 13108  

  cat source/bcc/treatment_chemotherapy_ohsu.json \
    source/bcc/treatment_chemotherapy_manually_entered.json \
    | wc -l
  >>> 9924  

  cat source/bcc/Radiotherapy.json | wc -l
  >>> 1888


GenetrailsVariant:
  cat  source/bcc/sample_genetrails_copy_number_variant.json \
    source/bcc/sample_genetrails_sequence_variant.json \
    | wc -l
  >>> 54183
  cat  source/bcc/sample_genetrails_copy_number_variant.json \
    source/bcc/sample_genetrails_sequence_variant.json \
    |  jq .ParticipantID  | sort -u | wc -l
  >>> 84  

```
