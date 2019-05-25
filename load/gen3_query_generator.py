"""Utility, creates projects and nodes.  Deletes existing nodes of input type by default."""
import os
from glob import glob
import json
import sys
from collections import defaultdict
from itertools import chain

from gen3_etl.utils.ioutils import reader
from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.gen3 import create_node, delete_all, submission_client, generate_edge_tablename, get_class_tablename_from_id
from gen3_etl.utils.collections import grouper

import networkx as nx
from networkx.algorithms.simple_paths import all_simple_paths
from  networkx.classes.function import neighbors



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


def node_pedigrees(schema):
    """Creates ancestors and descendants."""
    def pedigrees_default():
        return {'ancestors': set([]), 'descendants': set([])}
    pedigrees = defaultdict(pedigrees_default)
    link_lookup = {}
    for id, n in schema.items():
        if 'links' not in n or len(n['links']) == 0:
            continue
        links = n['links']
        if 'subgroup' in links[0]:
            links = links[0]['subgroup']
        for l in links:
            pedigrees[l['target_type']]['descendants'].add(id)
            link_lookup['{}-{}'.format(l['target_type'],id)] = l
    for id, n in pedigrees.copy().items():
        for descendant in n['descendants']:
            pedigrees[descendant]['ancestors'].add(id)
    return pedigrees, link_lookup


def generate_query(schema, node_type):
    pedigrees, link_lookup = node_pedigrees(schema)

    joins = []
    def process(id, child_id):
        dst_table_name = get_class_tablename_from_id(id)
        edge_table_name = None
        src_table_name = None
        if child_id:
            link = link_lookup.get('{}-{}'.format(id, child_id))
            edge_table_name = generate_edge_tablename(child_id, link['label'], link['target_type'])
            src_table_name = get_class_tablename_from_id(child_id)
        joins.append(
            {
             'child_id':child_id,
             'id':id,
             'dst_table_name': dst_table_name,
             'edge_table_name': edge_table_name,
             'src_table_name': src_table_name
             }
        )

    def traverse_up(id, child_id=None):
        process(id, child_id)
        for a in pedigrees[id]['ancestors']:
            traverse_up(a, id)

    traverse_up(node_type)

    sql = """
// {breadcrumb}
SELECT
    {node}.node_id as "{id}.node_id", {node}._props as "{id}",
{columns}
from {node}
{joins}
where {node}.node_id = ? ;"""

    def make_columns(path):
        # doesn't make 1st column :-(
        template = '{dst_table_name}.node_id as "{id}.node_id", {dst_table_name}._props as "{id}"' # "{dst_table_name}._props"
        return [template.format(**j['link']) for j in path if j['link']]

    def make_joins(path):
        # makes all joins :-)
        template = 'INNER JOIN {edge_table_name} ON ({edge_table_name}.src_id = {src_table_name}.node_id)'
        template2 = 'INNER JOIN {dst_table_name} ON ({edge_table_name}.dst_id = {dst_table_name}.node_id)'
        return list(
                    chain.from_iterable(
                        zip(
                            [template.format(**j['link']) for j in path if j['link']],
                            [template2.format(**j['link']) for j in path if j['link']]
                            )
                        )
                    )

    paths = all_paths(pedigrees, joins, node_type, 'program')
    queries = []
    for path in paths:
        col_clause = '    '+',\n    '.join(make_columns(path))
        join_clause = '    '+'\n    '.join(make_joins(path))
        breadcrumb = '->'.join([e['src_type'] for e in path])
        query = sql.format(columns=col_clause, joins=join_clause, node=joins[0]['dst_table_name'], id=joins[0]['id'], breadcrumb=breadcrumb)
        queries.append(query)
    return queries, pedigrees, link_lookup, joins, paths
    #return query

def all_paths(pedigrees, joins, src_type, dst_type):
    """Generates a set of all paths with associated link info, sorted."""
    G=nx.MultiDiGraph()
    G.add_nodes_from(pedigrees.keys())
    # https://networkx.github.io/documentation/networkx-1.9/reference/generated/networkx.MultiGraph.add_edges_from.html?highlight=add_edges_from#networkx.MultiGraph.add_edges_from
    G.add_edges_from([(j['child_id'],j['id'], j) for j in joins])
    # make a serialized string, so set can unique
    paths = set(['->'.join(p) for p in [p for p in all_simple_paths(G, src_type,dst_type)]])
    paths = [p.split('->') for p in sorted(paths)]

    for path in paths:
        enriched_path = []
        it = iter(path)
        dst = None
        for src in it:
            if dst:
                enriched_path.append( { 'src_type': dst, 'dst_type': src, 'link': G.get_edge_data(dst, src)[0] } )
            dst = next(it, None)
            edge_data = G.get_edge_data(src, dst)
            if edge_data:
                edge_data = edge_data[0]
            enriched_path.append( { 'src_type': src, 'dst_type': dst, 'link': edge_data } )
        yield enriched_path




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

    parser.add_argument('--node_type', type=str,
                        required=True,
                        help='generate a select for this node_type. e.g. "sample"')

    parser.add_argument('--credentials_path', type=str,
                        default=DEFAULT_CREDENTIALS_PATH,
                        help='Location of gen3 path ({}).'.format(DEFAULT_CREDENTIALS_PATH))

    parser.add_argument('--endpoint', type=str,
                        default=DEFAULT_ENDPOINT,
                        help='gen3 host base url ({}).'.format(DEFAULT_ENDPOINT))


    args = parser.parse_args()

    sc = submission_client(refresh_file=args.credentials_path, endpoint=args.endpoint)


    schema = get_schema(program=args.program,
        project=args.project,
        submission_client=sc,
        output_dir=args.output_dir,
    )

    queries, pedigrees, link_lookup, joins, paths = generate_query(schema, args.node_type)
    for query in queries:
        print(query)


    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    # paths = all_paths(pedigrees, 'submitted_file', 'program')
    # for path in paths:
    #     print('-')
    #     for node in path:
    #         print('{}->{}: {}'.format(node['src_type'], node['dst_type'], node['link']['edge_table_name']))
    #
    #
    # # [G.get_edge_data(p[0],p[1])[0]['edge_table_name'] for p in chunks(paths, 2)]
