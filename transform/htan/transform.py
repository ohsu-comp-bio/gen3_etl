"""Create gen3 json files in output/htan."""

from random import sample

from IPython.core.display import JSON
from gen3_etl.utils.ioutils import reader, JSONEmitter
from collections import defaultdict
# import inflect
# inflection = inflect.engine()
from parse import get_atlas_files
from pathlib import Path
import pandas 

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
    sample_id = line['HTAN ID']
    if line['BEMS ID'] != 'NA':
        bems_id = to_bems(line['BEMS ID'])
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

###
# Tracking manfiest functionality
###
def load_manifest(path='./source/htan/200618_HTAN ID_tracking manifest.xlsx'):
    '''
    Reads the excel file containing the tracking manifest, ignores the template 
    sheet, and concatenates the results before returning the manifest as a 
    Pandas dataframe
    '''
    # Read the dictionary {sheet_name: sheet_df} from the external file
    sheets = pandas.read_excel(path, sheet_name=None)
    # Exclude the template sheet, could cause need to DEBUG
    case_sheet_names = []
    for n in sheets.keys():
        if not 'keep' in n and not 'Template' in n:
            case_sheet_names.append(n)
    case_sheets = {n:sheets[n] for n in case_sheet_names}
    # Concatenate the dataframes in the array, reindex, and return
    manifest = pandas.concat(case_sheets, sort=True).reset_index()
    return manifest

def fix_columns(raw_manifest):
    '''
    Determine the appropriate institution label and appropriate assay label
    '''
    # Names of consortium organizations
    institutions = ['OHSU', 'HMS', 'MDACC']
    # Dictionary which associates researchers with their institutions
    researcherConversion = {
        'KDL': 'OHSU',
        'BETTS': 'OHSU',
        'CHIN': 'OHSU',
        'ADEY': 'OHSU',
        'NAVIN': 'MDACC',
        'SORGER': 'HMS',
        'RIESTERER': 'OHSU', 
        'COUSSENS': 'OHSU',
        'SPELLMAN': 'OHSU',
        'GRAY': 'OHSU'
    } 
    # Iterate over the rows of the dataframe determine the correct column values
    fixed_manifest = raw_manifest.astype(str)
    #fixed_manifest['BEMS ID'] = fixed_manifest['BEMS ID'].astype(float)
    #fixed_manifest['Parent BEMS ID'] = fixed_manifest['Parent BEMS ID'].astype(float)
    # Fix the values in each column
    for i, row in fixed_manifest.iterrows():
        # Fix Assay
        #fixed_manifest.at[i,'Assay'] = fixAssay(row['Assay'])
        fixed_manifest.at[i,'Assay'] = row['Assay']
        # Fix Institution
        # Check to see if there are reseacher names in the institution column
        for lab in researcherConversion:
            if lab in row['Institution'].upper():
                fixed_manifest.at[i,'Institution'] = researcherConversion[lab]
                fixed_manifest.at[i,'Lab'] = lab
                break
        # Otherwise, check for institutions names
        else:
            for inst in institutions:
                if inst in row['Institution'].upper():
                    fixed_manifest.at[i,'Institution'] = inst
                    fixed_manifest.at[i,'Lab'] = 'missing'
                    break
            # Otherwise, guess
            else:
                fixed_manifest.at[i,'Instituion'] = 'missing'
                fixed_manifest.at[i,'Lab'] = 'missing'
        # Fix BEMS ID
        bid = fixed_manifest['BEMS ID'].astype(float)[i]
        pbid = fixed_manifest['Parent BEMS ID'].astype(float)[i]
        #if pandas.isna(row['BEMS ID']) and not pandas.isna(row['Parent BEMS ID']):
        if pandas.isna(bid) and not pandas.isna(pbid):
            bid = pbid
        fixed_manifest.at[i,'BEMS ID'] = bid

    # Make the patients column
    patients = []
    for i, row in fixed_manifest.iterrows():
        if 'HTA' in row['HTAN ID']:
            patients.append('_'.join(row['HTAN ID'].split('_')[0:2]))
        elif 'HTA' in row['Parent HTAN ID']:
            patients.append('_'.join(row['Parent HTAN ID'].split('_')[0:2]))
        else:
            patients.append('Unidentified')
    fixed_manifest['Case'] = patients 

    # Make Experiment column
    fixed_manifest['Experiment'] = 'OMSAtlas'

    return fixed_manifest


def filter_rows(fixed_manifest):
    '''
    Drop the columns with NA in Institution, Assay, or HTAN ID
    *** HTAN ID might be missing for some of the rows that overlap sample ID***
    '''
    # Drop rows
    filtered_manifest = fixed_manifest.dropna(how='any', 
                              subset=['Institution','Assay','HTAN ID', 'BEMS ID', 'Parent BEMS ID'])
    # Remove NaNs from Assay
    filtered_manifest = filtered_manifest[filtered_manifest['Assay'] != 'nan']
    # ! Fix BEMS ID, this should be done in fix cols
    filtered_manifest['BEMS ID'] = filtered_manifest['BEMS ID'].astype(int).astype(str)
    filtered_manifest['BEMS ID'] = filtered_manifest['BEMS ID'].apply(lambda x: '0000'+x)
    return filtered_manifest[['Note' not in assay for assay in filtered_manifest.Assay]]


def process_manifest(manifest):
    '''
    Gets the raw manifest ready for use by filtering rows and fixing columns
    '''
    # Call function to ensure column assumptions hold
    fixed_columns = fix_columns(manifest)
    # Call function to appropriately subset rows. Also subset to useful columns
    columns = ['HTAN ID', 'Institution', 'Lab', 'Assay', 'BEMS ID', 'Case', 'Description', 'Experiment']
    processed_manifest = filter_rows(fixed_columns)[columns]
    return processed_manifest


manifest = process_manifest(load_manifest())
#print(manifest)
 



###
# Down to business
###

# not real anymore #keys = "Project	Experiment	Case	Sample	Slide	Aliquot	Alias	Description	Assay	Institution".split()
biopsy_pattern = re.compile(r'(?P<label>\S+)\s#(?P<id>\S+)\s(?P<description>.*)')


# load files
files = defaultdict(list)
for file in get_atlas_files():
    if 'Sample.bmes_id' not in file:
        print(f"warn:file_missing_bems /{file['uri']}")
        continue
    sample_id = file['Sample.bmes_id']
    files[sample_id].append(file)    


#for line in reader('source/htan/HTAN_Metadata.tsv'):
for i, line in manifest.iterrows():
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
