
from edge import transform, path_to_type, default_parser


if __name__ == "__main__":
    item_paths = ['source/reference/GeneOntologyIsA.Edge.json.gz']
    type = path_to_type(item_paths[0])
    args = default_parser().parse_args()
    transform(item_paths, output_dir=args.output_dir, type='edge_acffd5fd_geonteangeonte', project_id='smmart-reference')
