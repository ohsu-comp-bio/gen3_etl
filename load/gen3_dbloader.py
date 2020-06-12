"""Utility, creates projects and nodes.  Deletes existing nodes of input type by default."""
import os
from glob import glob
import json
import sys
from datetime import datetime

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

def write_edge(link, line, submission_client, project_id):
    """Writes edge file ready for sql import. Strips edge from submission node."""
    edges = line.get(link['src_edge_property'], None)
    if not edges:
        return line

    if not isinstance(edges, (list,)):
        edges = [edges]
    src_id = uuid.uuid5(uuid.NAMESPACE_DNS, line['submitter_id'].lower())

    for edge in edges:
        if link['src_edge_property'] == 'projects':
            dst_id = get_project_node_id(submission_client, project_id)
        else:
            dst_id = uuid.uuid5(uuid.NAMESPACE_DNS, edge.get('submitter_id', edge.get('code')).lower())
        link['handle'].write('{}\x01{}\x01{}\x01{}\x01{}\n'.format(src_id, dst_id, '{}', '{}', '{}'))
    del line[link['src_edge_property']]
    return line


def write_node(handle, line):
    """Writes edge file ready for sql import. Strips edge from submission node."""
    node_id = uuid.uuid5(uuid.NAMESPACE_DNS, line['submitter_id'].lower())
    now = datetime.utcnow().isoformat()
    # ensure timestamps
    for p in ['updated_datetime', 'created_datetime']:
        if p not in line:
            line[p] = now
    handle.write('{}\x01{}\x01{}\x01{}\n'.format(node_id, '{}', '{}', json.dumps(line, separators=(',',':'))))
    return line


def upload(path, program, project, submission_client, delete_first,output_dir):
    """Transforms submission record to node and edge files"""
    for p in glob(path):
        tables = None
        for line in reader(p):
            if 'project_id' not in line:
                line['project_id'] = '{}-{}'.format(program, project)
            assert 'project_id' in line, 'must have project_id'
            assert 'submitter_id' in line, 'must have submitter_id'
            if not tables:
                tables = get_tables(submission_client, line)
                tables['handle'] = open('{}/{}/{}.tsv'.format(output_dir, project, tables['node_table']), 'w')
                for l in tables['links']:
                    l['handle'] =  open('{}/{}/{}.tsv'.format(output_dir, project, l['edge_table']), 'w')
                if delete_first:
                    print("$psql -c \"delete from {} where _props->>'project_id' = '{}-{}'  ;\"".format(tables['node_table'], program, project))

            for l in tables['links']:
                line = write_edge(l, line, submission_client, '{}-{}'.format(program, project))
            write_node(tables['handle'], line)

        tables['handle'].close()
        node_path = '{}/{}/{}.tsv'.format(output_dir, project, tables['node_table'])
        print("cat  $DATA/{} | $psql -c \"copy {}(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\\x01' quote E'\\x02' ;\"".format(node_path, tables['node_table']))
        for l in tables['links']:
            l['handle'].close()
            edge_path = '{}/{}/{}.tsv'.format(output_dir, project, l['edge_table'])
            print("cat  $DATA/{} | $psql -c \"copy {}(src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\\x01' quote E'\\x02' ;\"".format(edge_path, l['edge_table']))


def get_tables(submission_client, line):
    """Consolidates node and edge table names"""
    assert 'type' in line, 'no "type" in record {}'.format(line)
    type = line['type']
    type_schema = submission_client.get_dictionary_node(type)
    assert 'id' in type_schema, '{} not found in schema {}'.format(type, type_schema)
    assert type == type_schema['id'], 'schema id should be the same as type {}'.format([type,type_schema['id']])
    table_name = get_class_tablename_from_id(type)
    tables = {'node_table': table_name, 'links': []}
    for link in type_schema['links']:
        src_edge_property = link['name']
        edge_table = generate_edge_tablename(type, link['label'], link['target_type'])
        tables['links'].append({'src_edge_property': src_edge_property, 'edge_table': edge_table})
    return tables

def get_project_node_id(submission_client, project_id):
    """Returns the node_id"""
    grapql = '{ project(project_id: "' +project_id+ '") { id } }'
    response = sc.query(grapql)
    return response['data']['project'][0]['id']




if __name__ == "__main__":
    parser = default_argument_parser(output_dir=DEFAULT_OUTPUT_DIR,
        description='Reads gen3 json ({}), creates psql tsv .'.format(DEFAULT_INPUT_DIR)
    )
    parser.add_argument('--path', type=str,
                        default=DEFAULT_PATH,
                        help='Match these files ({}).'.format(DEFAULT_PATH))
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

    upload(path=args.path,
           program=args.program,
           project=args.project,
           submission_client=sc,
           delete_first=args.delete_first,
           output_dir=args.output_dir,
           )
