from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID
from gen3_etl.utils.vertex import transform, default_parser, generate

import os


if __name__ == "__main__":
    item_paths = ['source/gdc/Case.Vertex.json.gz']
    type = 'demographic'
    args = default_parser(DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, DEFAULT_PROJECT_ID).parse_args()

    def my_callback(line):
        demographic = line['data']['gdc_attributes']['demographic']
        del line['data']['gdc_attributes']
        line['data'] = demographic
        return line



    transform(item_paths, output_dir=args.output_dir, type=type, callback=my_callback, project_id=args.project_id)
    p = os.path.join(args.output_dir, '{}.tsv'.format(type))
    assert os.path.isfile(p), 'should have an output file {}'.format(p)
    if args.schema:
        schema_path = generate(item_paths, type, output_dir=DEFAULT_OUTPUT_DIR)
        print(schema_path)
