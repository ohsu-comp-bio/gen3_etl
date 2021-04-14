"""Create gen3 json files in output/htan."""

from random import sample

from IPython.core.display import JSON
from gen3_etl.utils.ioutils import reader, JSONEmitter
from collections import defaultdict
# import inflect
# inflection = inflect.engine()
from parse import get_atlas_files
from pathlib import Path

entities = defaultdict(set)

import re


def get_id(k, line):
    """Get the key's value."""
    return line[k]


PROJECT_ID = "htan"

def project(line):
    """Render gen3 vertex."""
    return { "type": "project", "code": PROJECT_ID, "name": PROJECT_ID, "state": "open", "availability_type": "Open", "dbgap_accession_number": PROJECT_ID }


def experiment(line, project_id):
    """Render gen3 vertex."""
    submitter_id = get_id('Experiment', line)
    return {'type': "experiment", 'submitter_id': submitter_id, "projects": {"code": project_id}}    


def subject(line, project_id):
    """Render gen3 vertex."""
    submitter_id = get_id('Case', line)
    return {'type': "subject", 'submitter_id': submitter_id, "projects": {"code": project_id}}    


def sample(line, subject_id, biopsy_id, institution):
    """Render gen3 vertex."""
    bems_id = None
    sample_id = line['Sample']
    if line['Alias'] != 'NA':
        bems_id = to_bems(line['Alias'])
    _s = {'type': "sample", 'submitter_id': sample_id, "subjects": { "submitter_id": subject_id }}    
    if bems_id:
        _s["specimen_id"] = bems_id   
    if biopsy_id:
        _s["biopsy"] = biopsy_id
    if institution:
        _s["institution"] = institution
    return _s


def aliquot(line, bems_id, sample_id):
    """Render gen3 vertex."""
    return {'type': "aliquot", 'submitter_id': bems_id, "samples": { "submitter_id": sample_id }}    


SUFFIXES = ['am', 'avi', 'bin', 'czi', 'db', 'extrafeat', 'fastq', 'fq', 'jpg', 'log', 'metadata', 'mov', 'mpg', 'ome', 'other', 'png', 'rcanalysis', 'rcjob', 'rcpnl', 's0001_e00', 'shift', 'stc', 'svs', 'tif', 'txt', 'xlsx', 'xml', 'zip']

_suffixes = set()

_pipeline_stages = {
    "level1": ["czi", "fastq", "fq", "rcpnl", "svs", "tif", "tiff", "ome.tiff"],
    "level2": ["bam", "ome.tiff"],
    "level3": ["am", "tsv", "txt", "ome.tiff"],
    "level4": ["mov", "avi", "mpg", "tsv", "txt"]
}    


PIPELINE_EXTENSIONS = defaultdict(list)
for k, extensions in _pipeline_stages.items():
    for extension in extensions:
        PIPELINE_EXTENSIONS[extension].append(k)


def file_node(line, sample_id, aliquot_id):
    """Render gen3 vertex."""
    file_id = line['uri']

    suffix = None
    suffixes = Path(file_id).suffixes
    if len(suffixes) > 1 and suffixes[1] in ['.gz', '.tar']:
        suffix = suffixes[0]
    if len(suffixes) > 1 and suffixes[0] == 'ome' and suffixes[1] == 'tiff':
        suffix = 'ome.tiff'


    if len(suffixes) == 1:
        suffix = suffixes[0]
    if not suffix:
        suffix = "other"
    suffix = suffix.lower().replace('.', '')
    _suffixes.add(suffix)

    if suffix not in SUFFIXES:
        print(f"undefined suffix {suffix}, replacing with 'other'")
        suffix = "other"

    data_processing_stage = "unknown"
    if suffix in PIPELINE_EXTENSIONS:
        data_processing_stage = PIPELINE_EXTENSIONS[suffix][0]

    # task_input = aliquot_id or sample_id
    # task_id = f"task/{task_input}/{line['Task.id']}"
    # G.add_node(task_id, label='task')

    # task_definition_id = file['Task.id']
    # G.add_node(task_definition_id, label='panel')
    # if (task_definition_id, task_id ) not in edges:
    #     G.add_edge(task_definition_id, task_id, label='defines')
    #     edges.append((task_definition_id, task_id))


    _file = {'type': "file", 'submitter_id': file_id,
        "samples": { "submitter_id": sample_id },
        "file_name": line['DocumentReference.file_name'],
        "data_type": "Other",
        "data_format": suffix,
        "data_category": "Other",
        "file_size": line['size'],
        "md5sum": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "data_processing_stage": data_processing_stage,
        "data_processing_pipeline": line.get('Task.id', 'unknown')
    }
    if aliquot_id:
        _file['aliquots'] = { "submitter_id": aliquot_id }
    return _file    


written_nodes = []
emitters = {}

def write_node(node):
    """Emit node to filesystem."""
    try:
        type = node['type']
        _id = node.get('submitter_id', node.get('code', None))
        node_id = f"{type}/{_id}"
        if node_id not in written_nodes:
            if type not in emitters:
                emitters[type] = JSONEmitter(f"output/htan/{type}.ndjson",compresslevel=0)
            emitter = emitters[type]
            emitter.write(node)
            written_nodes.append(node_id)
        return _id
    except Exception as e:
        print(node)    
        raise e

def to_bems(alias):
    """Xform alias to bems."""
    return f"BEMS{alias.lstrip('0')}"


keys = "Project	Experiment	Case	Sample	Slide	Aliquot	Alias	Description	Assay	Institution".split()
biopsy_pattern = re.compile(r'(?P<label>\S+)\s#(?P<id>\S+)\s(?P<description>.*)')


# load files
files = defaultdict(list)
for file in get_atlas_files():
    if 'Sample.bmes_id' not in file:
        print(f"warn:file_missing_bems /{file['uri']}")
        continue
    sample_id = file['Sample.bmes_id']
    files[sample_id].append(file)    


for line in reader('source/htan/HTAN_Metadata.tsv'):    
    project_id = write_node(project(line))    
    experiment_id = write_node(experiment(line, project_id))
    subject_id = write_node(subject(line, project_id))

    biopsy_id = None
    biopsy_match = biopsy_pattern.match(line['Description'])
    if biopsy_match:
        biopsy = biopsy_match.groupdict()
        biopsy_id = f"{biopsy['id']}"

    _s = sample(line, subject_id, biopsy_id, line['Institution'])

    sample_id = write_node(_s)
    bems_id = _s.get('specimen_id', None)



    # biopsy_match = biopsy_pattern.match(line['Description'])
    # if biopsy_match:
    #     biopsy = biopsy_match.groupdict()
    #     biopsy_id = f"{case_id}/{sample_id}/{biopsy['id']}"
    #     G.add_node(biopsy_id, label='biopsy', description=biopsy['description'])
    #     if (case_id, biopsy_id) not in edges:
    #         G.add_edge(case_id, biopsy_id, label='biopsies')
    #         edges.append((case_id, biopsy_id))
    #     if (biopsy_id, sample_id) not in edges:
    #         G.add_edge(biopsy_id, sample_id, label='samples')
    #         edges.append((biopsy_id, sample_id))


    if bems_id:
        aliquot_id = bems_id
        write_node(aliquot(line, aliquot_id, sample_id))


    my_files = files.get(bems_id, None)
    if my_files:
        for _file in my_files:

            # task_input = aliquot_id or sample_id
            # task_id = f"task/{task_input}/{file['Task.id']}"
            # G.add_node(task_id, label='task')

            # task_definition_id = file['Task.id']
            # G.add_node(task_definition_id, label='panel')
            # if (task_definition_id, task_id ) not in edges:
            #     G.add_edge(task_definition_id, task_id, label='defines')
            #     edges.append((task_definition_id, task_id))

            # if (task_input, task_id) not in edges:
            #     G.add_edge(task_input, task_id, label='tasks')
            #     edges.append((task_input, task_id))

            # G.add_node(file['uri'], label='file', **file)
            # if (task_id, file['uri']) not in edges:
            #     G.add_edge(task_id, file['uri'], label='files')
            #     edges.append((task_id, file['uri']))
            write_node(file_node(_file, sample_id, aliquot_id))

        del files[bems_id]

for emitter in emitters.values():
    emitter.close()

# print(sorted(_suffixes))
# for bems_id in files:
#     for file in files[bems_id]:
#         G.add_node(f"file_bems_missing_from_spreadsheet/{file['uri']}", label='warn:file_bems_missing_from_spreadsheet')
