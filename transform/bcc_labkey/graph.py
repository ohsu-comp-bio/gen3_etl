import os
import re
import json
from pprint import pprint
from dateutil.parser import parse
from gen3_etl.utils.ioutils import reader
import hashlib

from datetime import date, datetime
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def compile(pattern):
  return re.compile(pattern, re.IGNORECASE)

stage_patterns = {
  '0': [compile('^0'), compile('^c0'), compile('^p0'), compile('^r0'), compile('^y0')],
  '1': [compile('^1'), compile('^c1'), compile('^p1'), compile('^r1'), compile('^y1')],
  '2': [compile('^2'), compile('^c2'), compile('^p2'), compile('^r2'), compile('^y2')],
  '3': [compile('^3'), compile('^c3'), compile('^p3'), compile('^r3'), compile('^y3')],
  '4': [compile('^4'), compile('^c4'), compile('^p4'), compile('^r4'), compile('^y4')],
}
stage_rating_patterns = {
  'A': [compile('^.{1,2}A')],
  'B': [compile('^.{1,2}B')],
  'C': [compile('^.{1,2}C')],
  'D': [compile('^.{1,2}D')],
  'E': [compile('^.{1,2}E')],
}
node_patterns = {
  '0': [compile('.*N0.*')],
  '1': [compile('.*N1.*')],
  '2': [compile('.*N2.*')],
  '3': [compile('.*N3.*')],
  '4': [compile('.*N4.*')],
  'X': [compile('.*NX.*')],
}
tumor_patterns = {
  '0': [compile('.*T0.*')],
  '1': [compile('.*T1.*')],
  '2': [compile('.*T2.*')],
  '3': [compile('.*T3.*')],
  '4': [compile('.*T4.*')],
  'X': [compile('.*TX.*')],
  'is': [compile('.*Tis.*')],
}
metastases_patterns = {
  '0': [compile('.*M0.*')],
  '1': [compile('.*M1.*')],
}
category_pattern = {
  'clinical': [compile('^c')],
  'pathologic': [compile('^p')],
  'recurrence': [compile('^r')],
  'autopsy': [compile('^a')],
}


def match_pattern(str, pattern_dict):
  if str:
    for k in pattern_dict:
      for pattern in pattern_dict[k]:
        if pattern.search(str):
          return k
  return None


def stage(str):
  return match_pattern(str, stage_patterns)

def stage_rating(str):
  return match_pattern(str, stage_rating_patterns)

def tumor(str):
  return match_pattern(str, tumor_patterns)

def node(str):
  return match_pattern(str, node_patterns)

def metastases(str):
  return match_pattern(str, metastases_patterns)

def category(str):
  return match_pattern(str, category_pattern)

def tnm(str):
  return {'stage': stage(str),
         'stage_rating': stage_rating(str),
         'tumor': tumor(str),
         'node': node(str),
         'metastases': metastases(str),
         'category': category(str),
         'source': str}


def convert(string, fuzzy=False, debug=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        return parse(string, fuzzy=fuzzy)
    except:
        if debug:
          print('could not parse', string)
        return string



item_paths = ['source/bcc/admindemographics.json',
              'source/bcc/demographics.json']
submitter_ids = {}
cases = {}

def case_lookup_values(paths):
    look_ups = {}
    for p in paths:
        c = p.replace('source/bcc/','').replace('genetrails_','').replace('.json','')
        look_ups[c] = {}
        for line in reader(p):
            name = line.get('display_name', line.get('case'))
            val = line.get('gender_id')
            if val == None or name == None:
                print(line)
            look_ups[c][val] = name
    return look_ups

case_lookup_paths = """
source/bcc/gender.json
""".strip().split()

case_lookups = case_lookup_values(case_lookup_paths)

def case_lookup(k, v):
    k = k.replace('_id', '')
    if k not in case_lookups:
        return v
    if v not in case_lookups[k]:
        return v
    return case_lookups[k][v]


def cases_ignore(k):
    if k.startswith('_'):
        return True
    if k.lower() in ['lsid']:
        return True
    return False

for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    submitter_ids[source] = []
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        assert participantid, line
        submitter_ids[source].append(participantid)
        if participantid not in cases:
          cases[participantid] = {'source': source, 'submitter_id': participantid }
        case = cases[participantid]
        for k in line:
          if cases_ignore(k):
            continue
          if 'date' in k.lower():
            case[k.lower()] = convert(line[k])
          else:
            v = case_lookup(k.lower(), line[k])
            if v:
                case[k.lower().replace('_id','')] = v
            else:
                case[k.lower()] = line[k]



item_paths = ['source/bcc/clinical_diagnosis.json',
              'source/bcc/clinical_narrative.json',
              'source/bcc/voncologdiagnosis.json']

def diagnosis_lookup_values(paths):
    look_ups = {}
    for p in paths:
        c = p.replace('source/bcc/','').replace('genetrails_','').replace('.json','').replace('diagnoses', 'diagnosis')
        look_ups[c] = {}
        for line in reader(p):
            name = line.get('display_name', line.get('diagnosis'))
            val = line.get('rowid', line.get('diagnosis_id') )
            if val == None or name == None:
                print(line)
            look_ups[c][val] = name
    return look_ups

diagnosis_lookup_paths = """
source/bcc/malignancy.json
source/bcc/diagnoses.json
""".strip().split()

diagnosis_lookups = diagnosis_lookup_values(diagnosis_lookup_paths)

def diagnosis_lookup(k, v):
    if k not in diagnosis_lookups:
        return v
    if v not in diagnosis_lookups[k]:
        return v
    return diagnosis_lookups[k][v]

def diagnosis_ignore(k):
    if k.startswith('_'):
        return True
    if k.lower() in ['list_container', 'list_entityid', 'list_lastindexed', 'lsid', 'participantid']:
        return True
    return False


for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        if participantid not in cases:
            print(f'source {source} participantid {participantid} not found in cases')
            continue
        case = cases[participantid]
        if 'diagnoses' not in case:
            case['diagnoses'] = []
        obj = {'source': source}
        for k in line:
            if diagnosis_ignore(k):
                continue
            if 'date' in k.lower():
                obj[k] = convert(line[k])
            else:
                obj[k] = diagnosis_lookup(k, line[k])
        d = None
        for df in ['date_of_diagnosis', 'diagnosis_date', 'date']:
            d = line.get(df, None)
            if d:
                break
        obj['timestamp'] = convert(d)
        assert obj['timestamp'], line
        for k in ['date_of_diagnosis', 'diagnosis_date', 'date']:
            if k in obj:
                del obj[k]

        if 'stage' in obj:
            obj['stage'] = tnm(obj['stage'])

        hash_object = hashlib.md5(json.dumps(obj, separators=(',',':'), default=json_serial).encode())

        obj['submitter_id'] = f"{case['participantid']}/{source}/{obj['timestamp']}/{hash_object.hexdigest()}"
        obj['type'] = 'diagnosis'
        case['diagnoses'].append(obj)



item_paths = ['source/bcc/sample.json']

sample_lookup_paths = """
source/bcc/sample_type.json
""".strip().split()

def sample_lookup_values(paths):
    look_ups = {}
    for p in paths:
        c = p.replace('source/bcc/','').replace('genetrails_','').replace('.json','').replace('sample_type', 'sample_type_id')
        look_ups[c] = {}
        for line in reader(p):
            name = line.get('display_name')
            val = line.get('sample_type_id')
            if val == None or name == None:
                print(line)
            look_ups[c][val] = name
    return look_ups

sample_lookups = sample_lookup_values(sample_lookup_paths)

def sample_lookup(k, v):
    if k not in sample_lookups:
        return v
    if v not in sample_lookups[k]:
        return v
    return sample_lookups[k][v]

def sample_ignore(k):
    return diagnosis_ignore(k)

for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        if participantid not in cases:
            cases[participantid] = {}
        case = cases[participantid]
        if 'samples' not in case:
            case['samples'] = []
        obj = {'source': source}
        for k in line:
            if sample_ignore(k):
                continue
            if 'date' in k.lower():
                obj[k] = convert(line[k])
            else:
                obj[k] = line[k]
        obj['sample_type'] = sample_lookup('sample_type_id', line['sample_type_id'])
        del obj['sample_type_id']
        obj['timestamp'] = convert(line.get('date', None))
        del obj['date']
        obj['submitter_id'] = f"{case['participantid']}/{obj['sample_code']}"
        obj['aliquots'] = [{'submitter_id': f"{obj['sample_code']}/aliquot", 'read_groups': []}]
        obj['type'] = 'sample'
        case['samples'].append(obj)



item_paths = ['source/bcc/biomarker_measurement_manually_entered.json',
              'source/bcc/biomarker_measurement_ohsu.json',
              'source/bcc/glycemic_lab_tests.json',
              'source/bcc/height_ohsu.json',
              'source/bcc/lesion_size.json',
              'source/bcc/weight_ohsu.json']

observation_lookup_paths = """
source/bcc/glycemic_unit_of_measure.json
source/bcc/glycemic_data_source.json
source/bcc/unit_of_measure.json
source/bcc/glycemic_assay.json
""".strip().split()

def observation_lookup_values(paths):
    look_ups = {}
    for p in paths:
        c = p.replace('source/bcc/','').replace('genetrails_','').replace('.json','')
        look_ups[c] = {}
        for line in reader(p):
            name = line.get('display_name')
            val = line.get(f'{c}_id')
            if val == None or name == None:
                print(line)
            look_ups[c][val] = name
    return look_ups

observation_lookups = observation_lookup_values(observation_lookup_paths)

def observation_lookup(k, v):
    k = k.replace('_id', '')
    if k not in observation_lookups:
        return v
    if v not in observation_lookups[k]:
        return v
    return observation_lookups[k][v]


def observation_ignore(k):
    return diagnosis_ignore(k)

for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        if participantid not in cases:
            print(f'source {source} participantid {participantid} not found in cases')
            continue
        case = cases[participantid]
        if 'observations' not in case:
            case['observations'] = []
        obj = {'source': source}
        for k in line:
            if observation_ignore(k):
                continue
            if 'date' in k.lower():
                obj[k] = convert(line[k])
            else:
                v = observation_lookup(k, line[k])
                if v:
                    obj[k.replace('_id','')] = v
                else:
                    obj[k] = line[k]
        obj['timestamp'] = convert(line.get('date', None))
        del obj['date']
        obj['submitter_id'] = f"{participantid}/{source}/{obj['timestamp']}"
        obj['type'] = 'observation'
        case['observations'].append(obj)


item_paths = ['source/bcc/treatment_chemotherapy_regimen.json',
              'source/bcc/treatment_chemotherapy_manually_entered.json',
              'source/bcc/treatment_chemotherapy_ohsu.json',
              'source/bcc/Radiotherapy.json']

treatment_lookup_paths = """
source/bcc/treatment_agent.json
source/bcc/treatment_agent_alt_name.json
source/bcc/treatment_chemotherapy_regimen.json
source/bcc/unit_of_measure.json
source/bcc/treatment_combo.json
source/bcc/treatment_combo_agents.json
source/bcc/treatment_type.json
source/bcc/delivery_method.json
""".strip().split()

def treatment_lookup_values(paths):
    look_ups = {}
    for p in paths:
        c = p.replace('source/bcc/','').replace('.json','')
        look_ups[c] = {}
        for line in reader(p):
            name = line.get('display_name', line.get('alt_display_name', None))
            for val in [line[k] for k in line if not k.startswith('_') and k.endswith('_id')]:
                look_ups[c][val] = name
    return look_ups

treatment_lookups = treatment_lookup_values(treatment_lookup_paths)

def treatment_lookup(k, v):
    k = k.replace('_id', '')
    if k not in treatment_lookups:
        return v
    if v not in treatment_lookups[k]:
        return v
    return treatment_lookups[k][v]

def treatment_ignore(k):
    ignore = diagnosis_ignore(k)
    if ignore:
        return ignore
    return k.endswith('_lsid')

for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        if participantid not in cases:
            cases[participantid] = {}
        case = cases[participantid]
        if 'treatments' not in case:
            case['treatments'] = []
        obj = {'source': source}
        for k in line:
            if treatment_ignore(k):
                continue
            if 'date' in k.lower():
                obj[k] = convert(line[k])
            else:
                v = treatment_lookup(k, line[k])
                if v:
                    obj[k.replace('_id','')] = v
                else:
                    obj[k] = line[k]
        obj['timestamp'] = convert(line.get('date', None))
        obj['submitter_id'] = f"{case['participantid']}/{source}/{obj['timestamp']}"
        obj['type'] = 'treatment'
        del obj['date']
        case['treatments'].append(obj)


def read_group_ignore(k):
    return diagnosis_ignore(k)

# run_status, assay_version
read_group_lookup_paths = """
source/bcc/genetrails_run_status.json
source/bcc/assay_version.json
""".strip().split()

def read_group_lookup_values(paths):
    look_ups = {}
    for p in paths:
        c = p.replace('source/bcc/','').replace('.json','').replace('genetrails_run_status','run_status')
        look_ups[c] = {}
        for line in reader(p):
            name = line.get('display_name', line.get('alt_display_name', None))
            for val in [line[k] for k in line if not k.startswith('_') and k.endswith('_id')]:
                look_ups[c][val] = name
    return look_ups

read_group_lookups = read_group_lookup_values(read_group_lookup_paths)

def read_group_lookup(k, v):
    k = k.replace('_id', '')
    if k not in read_group_lookups:
        return v
    if v not in read_group_lookups[k]:
        return v
    return read_group_lookups[k][v]


item_paths = ['source/bcc/sample_genetrails_assay.json']
for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        if participantid not in cases:
            cases[participantid] = {}
        case = cases[participantid]
        obj = {'source': source}
        for k in line:
            if read_group_ignore(k):
                continue
            if 'date' in k.lower():
                obj[k] = convert(line[k])
            else:
                v = read_group_lookup(k, line[k])
                if v:
                    obj[k.replace('_id','')] = v
                else:
                    obj[k] = line[k]
        obj['timestamp'] = obj['date']
        del obj['date']
        obj['type'] = 'read_group'
        obj['alleles'] = []
        # fix run status
        if 'run_status_id' in obj:
            obj['run_status'] = obj['run_status_id']
            del obj['run_status_id']
        if 'samples' not in case:
          case['samples'] = []
          # print(f"sample {obj['sample_code']} created in case {participantid}")
        found = False
        for s in case['samples']:
          if s['sample_code'] == obj['sample_code']:
            aliquot = s['aliquots'][0]
            if 'read_groups' not in aliquot:
              aliquot['read_groups'] = []
            aliquot['read_groups'].append(obj)
            found = True
        if not found:
          # print(f"genetrails_assay {obj['sample_code']} not found, created in case {participantid}")
          keys = ['sample_code', 'origin', 'timestamp', 'source']
          sample = {k: obj[k] for k in keys}
          sample['type'] = 'sample'
          sample['sample_type'] = 'Primary Tumor'
          sample['submitter_id'] = f"{case['participantid']}/{sample['sample_code']}"
          sample['aliquots'] = [{'submitter_id': f"{obj['sample_code']}/aliquot", 'read_groups': [obj]}]
          case['samples'].append(sample)

def gene_trails_ignore(k):
    return diagnosis_ignore(k)

# run_status, assay_version
gene_trails_lookup_paths = """
source/bcc/genetrails_classification.json
source/bcc/genetrails_copy_number_result_type.json
source/bcc/genetrails_protein_variant_type.json
source/bcc/genetrails_result_significance.json
source/bcc/genetrails_result_type.json
source/bcc/genetrails_run_status.json
source/bcc/genetrails_transcript_priority.json
source/bcc/genetrails_variant_type.json
source/bcc/chromosome.json
source/bcc/assay_categories.json
source/bcc/assay_version.json
source/bcc/gene.json
source/bcc/genome_build.json
""".strip().split()

def gene_trails_lookup_values(paths):
    look_ups = {}
    for p in paths:
        c = p.replace('source/bcc/','').replace('.json','').replace('genetrails_','')
        look_ups[c] = {}
        for line in reader(p):
            name = line.get('display_name', line.get('alt_display_name', None))
            for val in [line[k] for k in line if not k.startswith('_') and k.endswith('_id')]:
                look_ups[c][val] = name
    return look_ups

gene_trails_lookups = gene_trails_lookup_values(gene_trails_lookup_paths)

def gene_trails_lookup(k, v):
    k = k.replace('_id', '')
    if k not in gene_trails_lookups:
        return v
    if v not in gene_trails_lookups[k]:
        return v
    return gene_trails_lookups[k][v]


item_paths = [
    'source/bcc/sample_genetrails_sequence_variant.json',
    'source/bcc/sample_genetrails_copy_number_variant.json',
]
for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        if participantid not in cases:
            print(f'{participantid} not found')
            continue
        case = cases[participantid]
        if 'samples' not in case:
            print(f'{participantid} has no samples')
            continue
        sample_code = line["assay_lsid/sample_code"]
        found = False
        for s in case['samples']:
            if s['sample_code'] == sample_code:
                found = True
                aliquot = s['aliquots'][0]
                if 'read_groups' not in aliquot:
                    print(f'{participantid} {sample_code} {aliquot} has no read_group?')
                else:
                    for k in [k for k in line if '/' in k]:
                        line[k.split('/')[1]] = line[k]
                        del line[k]
                    obj = {'source': source}
                    for k in line:
                        if gene_trails_ignore(k):
                            continue
                        if 'date' in k.lower():
                            obj[k] = convert(line[k])
                        else:
                            v = gene_trails_lookup(k, line[k])
                            if v:
                                obj[k.replace('_id','')] = v
                            else:
                                obj[k] = line[k]
                    obj['timestamp'] = obj['date']
                    del obj['date']
                    sample['type'] = 'allele'
                    read_group = aliquot['read_groups'][0]
                    if 'alleles' not in read_group:
                        read_group['alleles'] = []
                    read_group['alleles'].append(obj)

        if not found:
            print(f'{participantid} has no samples for {sample_code}')


cases_ids = set(cases)
admin_ids = set(submitter_ids['admindemographics'])
demographics_ids = set(submitter_ids['demographics'])

diagnoses_ids = set([c[1]['participantid'] for c in cases.items() if 'diagnoses' in c[1] and len(c[1]['diagnoses']) > 1 ])
observations_ids = set([c[1]['participantid'] for c in cases.items() if 'observations' in c[1] and len(c[1]['observations']) > 1 ])

read_groups_ids = set()
for c in cases.items():
    case = c[1]
    if 'samples' in case:
        for s in case['samples']:
            if 'read_groups' in s:
                read_groups_ids.add(case['participantid'])

print('cases without admin', len(cases_ids - admin_ids))
print('demographics without admin', len(demographics_ids - admin_ids) )
print('admin without diagnoses',  len(admin_ids - diagnoses_ids))
print('admin without observations',  len(admin_ids - observations_ids))
print('admin without read_groups',  len(admin_ids - read_groups_ids))


project_id = 'ohsu-bcc'



with open('output/bcc/graph.json', 'w') as output:
    for case_id, case in cases.items():
        assert 'participantid' in case, case
        json.dump(case, output, separators=(',',':'), default=json_serial)
        output.write('\n')
