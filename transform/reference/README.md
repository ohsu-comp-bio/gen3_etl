
```
# download
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/ensembl/Gene.Vertex.json.gz .

# transform
python3 transform/reference/gene.py

# bulk insert node
cat output/reference/gene.tsv  | dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy node_gene(node_id, _props) from stdin  with delimiter E'\t' ;"
cat ~/gen3_etl/output/reference/gene.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy node_gene(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

# set misc props
update node_gene set acl = '{}', _sysan  = '{}' ;


insert into
  edge_genebelongstoproject(src_id, dst_id, acl,  _sysan,  _props)
select
  node_id  as "src_id" ,
  (select node_id from node_project where _props->>'code' = 'reference') as "dst_id",
  '{}' as "acl",
  '{}' as "_sysan",
  '{}' as "_props"
from node_gene   ;

```

# GeneOntologyTerm
```

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/go/GeneOntologyTerm.Vertex.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/go/GeneOntologyAnnotation.Edge.json.gz .
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/go/GeneOntologyIsA.Edge.json.gz .

# transform
python3 transform/reference/vertex.py

# manually adjust schema, including links

# restart sheepdog
# examine schema to get node_* and edge_* table names
dc exec --user postgres postgres  pg_dump -t 'node_geneontologyterm' --schema-only   -d metadata_db
dc exec --user postgres postgres  psql  -d metadata_db -c "\dt;"


# load vertex (node)

cat ~/gen3_etl/output/reference/gene_ontology_term.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy node_geneontologyterm(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

# load edge (link)


cat ~/gen3_etl/output/reference/edge_geneontologytermannotationsgene.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy edge_geneontologytermannotationsgene(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"


cat ~/gen3_etl/output/reference/edge_acffd5fd_geonteangeonte.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy edge_geneontologytermisageneontologyterm(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"



```


# Phenotype

```

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/phenotype/normalized.Phenotype.Vertex.json.gz source/reference/

# transform
python3 transform/reference/phenotype.py

# adjust schema manually, restart sheepdog

# load vertex (node)

cat ~/gen3_etl/output/reference/phenotype.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy node_phenotype(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"


# load edge in reference project

insert into
  edge_phenotypebelongstoproject(src_id, dst_id, acl,  _sysan,  _props)
select
  node_id  as "src_id" ,
  (select node_id from node_project where _props->>'code' = 'reference') as "dst_id",
  '{}' as "acl",
  '{}' as "_sysan",
  '{}' as "_props"
from node_phenotype   ;


edge_diagnosisinstanceofphenotype


```

# allele

```

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/allele/Allele.Vertex.json.gz source/reference/

# transform
python3 transform/reference/allele.py

# load

cat ~/gen3_etl/output/reference/allele.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy node_allele(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"


cat /tmp/allele.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy node_allele(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

cat /tmp/allele_to_gene.tsv   \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy edge_geneinallele(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"

```


#

```
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/compound/normalized.Compound.Vertex.json.gz source/reference/

scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/compound/normalized.TreatedWith.Edge.json.gz source/reference/
scp ubuntu@neo4j:/mnt/bmeg/bmeg-etl/outputs/compound/normalized.TestedIn.Edge.json.gz source/reference/

zcat < source/reference/normalized.Compound.Vertex.json.gz | head -1 | jq .



python3 transform/reference/case_compound_edge.py


cat ~/gen3_etl/output/reference/compound.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db  -c "copy node_compound(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"



cat ~/gen3_etl/output/reference/edge_casetreatedwithcompound.tsv \
| dc exec  -T --user postgres postgres  psql  -d metadata_db \
-c "copy edge_casetreatedwithcompound(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"


```
