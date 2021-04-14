"""Read spreadsheet, link with directory listing."""
from random import sample
from gen3_etl.utils.ioutils import reader
from collections import defaultdict
# import inflect
# inflection = inflect.engine()
from parse import get_atlas_files

entities = defaultdict(set)

import networkx as nx
import re

def add_node(k, line, G):
    """Create a vertex from a key and dict."""
    label = k.lower()
    id = f"{label}/{line[k]}"
    G.add_node(id, label=label)
    return id

def to_bems(alias):
    """Xform alias to bems."""
    return f"BEMS{alias.lstrip('0')}"


G = nx.MultiDiGraph()
keys = "Project	Experiment	Case	Sample	Slide	Aliquot	Alias	Description	Assay	Institution".split()
biopsy_pattern = re.compile(r'(?P<label>\S+)\s#(?P<id>\S+)\s(?P<description>.*)')


# load files
files = defaultdict(list)
for file in get_atlas_files():
    if 'Sample.bmes_id' not in file:
        G.add_node(f"file_missing_bems/{file['uri']}", label='warn:file_missing_bems')
        continue
    sample_id = file['Sample.bmes_id']
    files[sample_id].append(file)    


# dedup since we are using MultiDiGraph
edges = []

for line in reader('source/htan/HTAN_Metadata.tsv'):    
    project_id = add_node('Project', line, G)    
    experiment_id = add_node('Experiment', line, G)
    if (project_id, experiment_id) not in edges:
        G.add_edge(project_id, experiment_id, label='experiments')
        edges.append((project_id, experiment_id))

    case_id = add_node('Case', line, G)
    if (experiment_id, case_id) not in edges:
        G.add_edge(experiment_id, case_id, label='cases')
        edges.append((experiment_id, case_id))

    sample_id = f"sample/{line['Sample']}"
    bems_id = None
    if line['Alias'] != 'NA':
        bems_id = to_bems(line['Alias'])
    G.add_node(sample_id, label='sample', description=line['Description'], sample_id={line['Sample']}, bems_id=bems_id)
    if (case_id, sample_id) not in edges:
        G.add_edge(case_id, sample_id, label='samples')
        edges.append((case_id, sample_id))

    biopsy_match = biopsy_pattern.match(line['Description'])
    if biopsy_match:
        biopsy = biopsy_match.groupdict()
        biopsy_id = f"{case_id}/{sample_id}/{biopsy['id']}"
        G.add_node(biopsy_id, label='biopsy', description=biopsy['description'])
        if (case_id, biopsy_id) not in edges:
            G.add_edge(case_id, biopsy_id, label='biopsies')
            edges.append((case_id, biopsy_id))
        if (biopsy_id, sample_id) not in edges:
            G.add_edge(biopsy_id, sample_id, label='samples')
            edges.append((biopsy_id, sample_id))

    if bems_id:
        aliquot_id = f"aliquot/{bems_id}"
        G.add_node(aliquot_id, label='aliquot')
        if (sample_id, aliquot_id) not in edges:
            G.add_edge(sample_id, aliquot_id, label='aliquots')
            edges.append((sample_id, aliquot_id))


    my_files = files.get(bems_id, None)
    if my_files:
        for file in my_files:

            task_input = aliquot_id or sample_id
            task_id = f"task/{task_input}/{file['Task.id']}"
            G.add_node(task_id, label='task')

            task_definition_id = file['Task.id']
            G.add_node(task_definition_id, label='panel')
            if (task_definition_id, task_id ) not in edges:
                G.add_edge(task_definition_id, task_id, label='defines')
                edges.append((task_definition_id, task_id))

            if (task_input, task_id) not in edges:
                G.add_edge(task_input, task_id, label='tasks')
                edges.append((task_input, task_id))

            G.add_node(file['uri'], label='file', **file)
            if (task_id, file['uri']) not in edges:
                G.add_edge(task_id, file['uri'], label='files')
                edges.append((task_id, file['uri']))
        del files[bems_id]

for bems_id in files:
    for file in files[bems_id]:
        G.add_node(f"file_bems_missing_from_spreadsheet/{file['uri']}", label='warn:file_bems_missing_from_spreadsheet')


import matplotlib.pyplot as plt
import humanfriendly
import pygraphviz
from IPython.display import Image


def summarize_graph(graph):
    """Introspect the data in the graph, creates a summary graph.  Relies on label attribute on each node."""
    # calc labels and edge lables
    labels = defaultdict(int)
    sizes = defaultdict(int)

    for k, v in graph.nodes.data():
        labels[v['label']] += 1
        if 'size' in v:
            sizes[v['label']] += v['size']

    for k, v in labels.items():
        labels[k] = '{}({})'.format(k, v)

    for k, v in labels.items():
        if k in sizes:
            labels[k] = '{}({})'.format(v, humanfriendly.format_size(sizes[k]))

    edge_labels = defaultdict(int)
    for n in graph.nodes():
        label = graph.nodes[n]['label']
        for neighbor in graph.neighbors(n):
            n_lable = graph.nodes[neighbor]['label']
            edges = graph.get_edge_data(n, neighbor)
            for e in edges.values():
                edge_labels[(label, n_lable, e['label'])] += 1
    for k, v in edge_labels.items():
        edge_labels[k] = '{}'.format(v)

    # create new summary graph
    g = pygraphviz.AGraph(strict=False, directed=True)

    for k in labels:
        g.add_node(k, label=labels[k])

    compass_points = [('e', 'w')]

    for k in edge_labels:
        start = k[0]
        end = k[1]
        # key = k[2]
        # use compass points for self loops
        opts = {}
        if start == end:
            compass_point_offset = len([e for e in g.out_edges([start]) if e[1] == start]) % len(compass_points)
            compass_point = compass_points[compass_point_offset]
            opts = {'headport': compass_point[1], 'tailport': compass_point[0]}
        g.add_edge(start, end, label=f'{k[2]}({edge_labels[k]})', labeldistance=0, **opts)

    return g


def draw_summary(g, label='<untitled>', prog='dot', name=None, save_dot_file=False, size='40,40', scale=3):
    """Create network graph figure using pygraphviz."""
    # ['dot', 'neato', 'twopi', 'circo', 'fdp', 'sfdp']
    g.layout(prog)
    g.graph_attr.update(label=label, size=size, pad=1)
    g.edge_attr.update(arrowsize='0.6', style='dotted')
    g.graph_attr.update(scale=scale)  # , nodesep=1, ratio='auto')
    # if not set, default to first word in label
    if not name:
        name = label.split()[0]
    if save_dot_file:
        g.write(f'{name}.dot')
    g.draw(f'{name}.png')
    return Image(f'{name}.png')

# for k, v in G.nodes.data():
#     print(k,v)


draw_summary(summarize_graph(G), label='htan', save_dot_file=True)
