

"""
CREATE TABLE public.node_case (
    created timestamp with time zone DEFAULT now() NOT NULL,
    acl text[],
    _sysan jsonb,
    _props jsonb,
    node_id text NOT NULL
);

{
  "_id": "ENSG00000223972",
  "gid": "ENSG00000223972",
  "label": "Gene",
  "data": {
    "gene_id": "ENSG00000223972",
    "symbol": "DDX11L1",
    "description": "DEAD/H (Asp-Glu-Ala-Asp/His) box helicase 11 like 1 [Source:HGNC Symbol;Acc:37102]",
    "chromosome": "1",
    "start": 11869,
    "end": 14412,
    "strand": "+",
    "genome": "GRCh37"
  }
}
"""
import hashlib
import os
import json
import uuid

# make a UUID using a SHA-1 hash of a namespace UUID and a name

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, default_parser, emitter
from gen3_etl.utils.schema import generate, template


def transform_old(item_paths, output_dir, experiment_code, compresslevel=0):
    """Read bcc labkey json and writes gen3 json."""
    genes_emitter = emitter('gene', output_dir=output_dir)
    genes = {}
    for p in item_paths:
        for line in reader(p):
            case = {'type': 'gene', 'experiments': {'submitter_id': experiment_code}, 'submitter_id': line['participantid']}
            if line['participantid'] in genes:
                # print('merge', line['participantid'])
                case = genes[line['participantid']]
            case.update(line)
            genes[line['participantid']] = case
    for k in genes:
        genes_emitter.write(genes[k])
    genes_emitter.close()


def transform(item_paths, output_dir, project_id, compresslevel=0):
    """Read bcc labkey json and writes postgres TSV with embedded gen3 json."""
    path = os.path.join(output_dir, 'gene.tsv')
    lookup_path = os.path.join(output_dir, 'gene_lookup.tsv')
    with open(lookup_path, 'w') as lookup_file:
        with open(path, 'w') as output_file:
            for p in item_paths:
                for line in reader(p):
                    gene_id = line['data']['gene_id'].lower()
                    symbol = line['data']['symbol'].lower()
                    node_id = uuid.uuid5(uuid.NAMESPACE_DNS, gene_id)
                    line['data']['project_id'] = project_id
                    line['data']['submitter_id'] = gene_id
                    # copy node_gene(node_id, acl, _sysan,  _props) from stdin  with delimiter E'\t' ;
                    output_file.write('{}\x01{}\x01{}\x01{}\n'.format(node_id, '{}', '{}', json.dumps(line['data'], separators=(',',':'))))
                    lookup_file.write('{}\t{}\n'.format(symbol, gene_id))


if __name__ == "__main__":
    item_paths = ['source/reference/Gene.Vertex.json.gz']
    args = default_parser().parse_args()
    transform(item_paths, output_dir=args.output_dir, project_id='smmart-reference')
    if args.schema:
        schema_path = generate(item_paths,'gene', output_dir=DEFAULT_OUTPUT_DIR)
        print(schema_path)
