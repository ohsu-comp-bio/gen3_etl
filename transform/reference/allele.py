from  vertex import transform, default_parser, generate, DEFAULT_OUTPUT_DIR
import uuid


if __name__ == "__main__":
    item_paths = ['source/reference/Allele.Vertex.json.gz']
    type = 'allele'
    args = default_parser().parse_args()

    # load the symbol:submitter_id
    gene_lookup = {}
    with open('output/reference/gene_lookup.tsv') as f:
        gene_lookup = {k: v for k,v in (line.split() for line in f) }

    # write the edge via callback
    allele_to_gene =  open('output/reference/allele_to_gene.tsv', 'w')

    def my_callback(line):
        symbol = line['data'].get('hugo_symbol', None)
        gene_submitter_id = gene_lookup.get(symbol.lower(), None)
        if not symbol or not gene_submitter_id:
            print('missing gene symbol: {}, submitter_id: {}'.format(symbol, gene_submitter_id))
        else:
            src_id = line['node_id']
            dst_id = uuid.uuid5(uuid.NAMESPACE_DNS, gene_submitter_id)
            allele_to_gene.write('{}\x01{}\x01{}\x01{}\x01{}\n'.format(src_id, dst_id, '{}', '{}', '{}'))
        return line


    transform(item_paths, output_dir=args.output_dir, type=type, callback=my_callback, project_id='smmart-reference')

    allele_to_gene.close()

    if args.schema:
        schema_path = generate(item_paths, type, output_dir=DEFAULT_OUTPUT_DIR)
        print(schema_path)
