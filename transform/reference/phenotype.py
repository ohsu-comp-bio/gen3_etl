from  vertex import transform, default_parser, generate, DEFAULT_OUTPUT_DIR

if __name__ == "__main__":
    item_paths = ['source/reference/normalized.Phenotype.Vertex.json.gz', 'source/reference/phenotype_from_bcc_diagnosis.json']
    type = 'phenotype'
    args = default_parser().parse_args()
    transform(item_paths, output_dir=args.output_dir, type=type, project_id='smmart-reference')
    if args.schema:
        schema_path = generate(item_paths, type, output_dir=DEFAULT_OUTPUT_DIR)
        print(schema_path)
