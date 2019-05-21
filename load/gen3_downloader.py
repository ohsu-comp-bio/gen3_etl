"""Utility, creates projects and nodes.  Deletes existing nodes of input type by default."""
import os
from glob import glob
import json
import sys

from gen3_etl.utils.ioutils import reader
from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.gen3 import create_node, delete_all, submission_client, generate_edge_tablename, get_class_tablename_from_id
from gen3_etl.utils.collections import grouper

DEFAULT_INPUT_DIR = 'output'
DEFAULT_OUTPUT_DIR = 'output'
DEFAULT_PROGRAM = 'ohsu'
DEFAULT_PROJECT = None
DEFAULT_PATH = '**/*.json*'
DEFAULT_BATCH_SIZE = 100
DEFAULT_DELETE_FIRST = False

DEFAULT_CREDENTIALS_PATH = os.path.join('config', 'credentials.json')
DEFAULT_ENDPOINT = 'https://localhost'


import logging as logger
import uuid


def download(program, project, submission_client,output_dir, delete_first=False):
    """Downloads all populated nodes."""
    for node_type in node_types(submission_client):
        # print(help(submission_client))
        # submission_client.export_node(program, project, node_type, 'json', filename='{}/{}.json'.format(output_dir, node_type))
        filename = '{}/{}/{}.json'.format(output_dir, project, node_type)
        if os.path.isfile(filename) and not delete_first:
            print('{} exists, skipping.'.format(filename))
            continue
        print('downloading {}'.format(node_type))
        nodes = submission_client.export_node(program, project, node_type, 'json')
        # nodes = submission_client.export_node_all_type(program, project, node_type)
        if 'data' not in nodes:
            print('\t{} no data {}'.format(node_type, nodes))
            continue
        nodes = nodes['data']
        if len(nodes) == 0:
            print('\tlength 0 skipping'.format())
            continue
        else:
            print('\t{} length {}'.format(filename, len(nodes)))
        with open(filename, 'w') as output:
            for n in nodes:
                output.write(json.dumps(n,  separators=(',',':')))
                output.write('\n')
    filename = '{}/{}/{}.json'.format(output_dir, project, 'schema')
    if os.path.isfile(filename):
        print('{} exists, skipping.'.format(filename))
    else:
        print('downloading {}'.format('schema'))
        schema = submission_client.get_dictionary_all()
        with open(filename, 'w') as output:
            output.write(json.dumps(schema,  separators=(',',':')))
            output.write('\n')

def get_schema(program, project, submission_client,output_dir):
    """Returns cached copy of project's schema."""
    schema = None
    filename = '{}/{}/{}.json'.format(output_dir, project, 'schema')
    if os.path.isfile(filename):
        with open(filename, 'r') as input:
            schema = json.loads(input.read())
    else:
        print('downloading {}'.format('schema'))
        schema = submission_client.get_dictionary_all()
        with open(filename, 'w') as output:
            output.write(json.dumps(schema,  separators=(',',':')))
            output.write('\n')
    return schema



def nodes_in_load_order(program, project, submission_client,output_dir):
    """Introspects schema and returns types in order of db load."""
    schema = get_schema(program, project, submission_client,output_dir)
    loaded = {}

    def process(current, depth):
        loaded[current['id']] = depth

    def traverse(current, depth=0, depth_limit=1):
        if depth > depth_limit:
            return
        process(current, depth)
        target_type = current['id']
        for k in schema.keys():
            n = schema[k]
            if 'links' not in n or len(n['links']) == 0:
                continue
            links = n['links']
            if 'subgroup' in links[0]:
                links = links[0]['subgroup']
            for l in links:
                if l['target_type'] == target_type:
                    process(schema[n['id']], depth)
            for l in links:
                if l['target_type'] == target_type:
                    traverse(schema[n['id']], depth+1, depth_limit)
            depth_limit += 1

    loadables = [k for k in schema.keys() if not k.startswith('_') and not schema[k].get('category', None) == 'internal' ]
    current = [schema[l] for l in loadables if schema[l].get('links', None) == []][0]

    traverse(current)
    load_order = []
    levels = set([v for k,v in loaded.items()])
    for i in sorted(levels):
        for k,v in loaded.items():
            if v == i:
                load_order.append(k)
    return load_order


def files_in_load_order(program, project, submission_client,output_dir):
    nodes = nodes_in_load_order(program, project, submission_client,output_dir)
    files = ['{}/{}/{}.json'.format(output_dir,project,n) for n in nodes if os.path.isfile('{}/{}/{}.json'.format(output_dir,project,n))]
    return files



def node_types(submission_client):
    """Consolidates node and edge table names"""
    type_schema = submission_client.get_dictionary_all()
    return [k for k in type_schema.keys() if not k.startswith('_')]


"""
  export EP='--endpoint https://nci-crdc-demo.datacommons.io'
  export PP='--program DCF  --project CCLE --credentials_path config/datacommons.io.credentials.json'
"""




if __name__ == "__main__":
    parser = default_argument_parser(output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads gen3 json ({}), creates psql tsv .'.format(DEFAULT_INPUT_DIR)
    )
    parser.add_argument('--program', type=str,
                        default=DEFAULT_PROGRAM,
                        help='Name of existing gen3 program ({}).'.format(DEFAULT_PROGRAM))
    parser.add_argument('--project', type=str,
                        default=DEFAULT_PROJECT,
                        required=True,
                        help='Name of existing gen3 project ({}).'.format(DEFAULT_PROJECT))

    parser.add_argument('--delete_first', type=bool,
                        default=DEFAULT_DELETE_FIRST,
                        help='Delete all types from project before upload?({}).'.format(DEFAULT_DELETE_FIRST))

    parser.add_argument('--credentials_path', type=str,
                        default=DEFAULT_CREDENTIALS_PATH,
                        help='Location of gen3 path ({}).'.format(DEFAULT_CREDENTIALS_PATH))

    parser.add_argument('--endpoint', type=str,
                        default=DEFAULT_ENDPOINT,
                        help='gen3 host base url ({}).'.format(DEFAULT_ENDPOINT))


    args = parser.parse_args()

    sc = submission_client(refresh_file=args.credentials_path, endpoint=args.endpoint)


    download(
           program=args.program,
           project=args.project,
           submission_client=sc,
           output_dir=args.output_dir,
           delete_first=args.delete_first
           )


    # nodes = nodes_in_load_order(
    #        program=args.program,
    #        project=args.project,
    #        submission_client=sc,
    #        output_dir=args.output_dir,
    #        )
    #
    # print('\n'.join(nodes))

    files = files_in_load_order(
           program=args.program,
           project=args.project,
           submission_client=sc,
           output_dir=args.output_dir,
    )
    print('\n'.join(files))
