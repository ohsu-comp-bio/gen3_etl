
from edge import transform, path_to_type, default_parser


"""
{
  "_id": "(GO:0003924)--GeneOntologyAnnotation->(ENSG00000244115)",
  "gid": "(GO:0003924)--GeneOntologyAnnotation->(ENSG00000244115)",
  "label": "GeneOntologyAnnotation",
  "from": "GO:0003924",
  "to": "ENSG00000244115",
  "data": {
    "evidence": "IEA",
    "title": "Guanine nucleotide-binding protein subunit gamma",
    "references": [
      "GO_REF:0000002"
    ]
  }
}

select * from edge_geneontologytermannotationsgene ;
 created | acl | _sysan | _props | src_id | dst_id
---------+-----+--------+--------+--------+--------

"""

if __name__ == "__main__":
    item_paths = ['source/reference/GeneOntologyAnnotation.Edge.json.gz']
    type = path_to_type(item_paths[0])
    args = default_parser().parse_args()
    gene_lookup = set([])
    with open('output/reference/gene_lookup.tsv') as f:
        for line in f:
            gene_lookup.add(line.split()[1])
    def my_filter(line):
        """Filters out genes we have no identifier for."""
        if line['to'].lower() in gene_lookup:
            return True
        return False
    transform(item_paths, output_dir=args.output_dir, type='edge_geneontologytermannotationsgene', project_id='smmart-reference', filter=my_filter)
