from  vertex import transform, path_to_type, default_parser, generate, DEFAULT_OUTPUT_DIR

if __name__ == "__main__":
    item_paths = ['source/reference/GeneOntologyTerm.Vertex.json.gz']
    type = path_to_type(item_paths[0])
    args = default_parser().parse_args()
    transform(item_paths, output_dir=args.output_dir, type=type, project_id='smmart-reference')
    if args.schema:
        schema_path = generate(item_paths, type, output_dir=DEFAULT_OUTPUT_DIR)
        print(schema_path)
