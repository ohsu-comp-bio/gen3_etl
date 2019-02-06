To retrieve bmeg data

```
cd source/ccle


scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/BiosampleFor.Edge.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.Biosample.Vertex.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.BiosampleFor.Edge.json.gz .


scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/Biosample.Vertex.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/BiosampleFor.Edge.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.Biosample.Vertex.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.BiosampleFor.Edge.json.gz .

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/Aliquot.Vertex.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/AliquotFor.Edge.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.Aliquot.Vertex.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.AliquotFor.Edge.json.gz .

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/CallsetFor.Edge.json.gz  .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.CallsetFor.Edge.json.gz  .

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/InProject.Edge.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.InProject.Edge.json.gz .

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/Individual.Vertex.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/maf.Individual.Vertex.json.gz .

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/DerivedFrom.Edge.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ccle/File.Vertex.json.gz  .
```

To transform

```
python3 transform/ccle/cases.py
python3 transform/ccle/samples.py
python3 transform/ccle/read_groups.py
python3 transform/ccle/files.py
```

To delete

```
python3 load/gen3_deleter.py --program smmart --project ccle
```

To load

```
python3 load/gen3_deleter.py --program smmart --project ccle

python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/projects.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/experiments.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/cases.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/demographics.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/diagnosis.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/samples.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/aliquots.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/read_group.json
python3 load/gen3_loader.py --program smmart --project ccle --path output/ccle/submitted_somatic_mutation.json
```
