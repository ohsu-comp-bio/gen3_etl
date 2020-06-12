import os
import re
import json
import sys
import hashlib
from pprint import pprint
from dateutil.parser import parse
from gen3_etl.utils.ioutils import reader
from collections import defaultdict


from datetime import date, datetime
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))



cases = {}
item_paths = ['output/bcc/graph.json']
for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        cases[participantid] = line

def convert(date):
    return date


item_paths = ['source/bcc/vbiolibraryspecimens.json']


aliquots_ids = {}

def aliquot_ignore(key):
    if k.startswith('_'):
        return True
    if k.lower() in ['list_container', 'list_entityid', 'list_lastindexed', 'lsid', 'participantid']:
        return True

for p in item_paths:
    source = os.path.splitext(os.path.basename(p))[0]
    for line in reader(p):
        participantid = line.get('participantid', line.get('ParticipantID', None))
        if participantid not in cases:
            print(f'source {source} participantid {participantid} not found in cases', file=sys.stderr)
            continue
        case = cases[participantid]
        if 'samples' not in case:
            # print(f'source {source} participantid {participantid} has no samples')
            case['samples'] = []

        pathology_record_number = line['pathology_record_number']
        sample_bems_id = None
        sample = None
        if not pathology_record_number:
            # print(f'source {source} participantid {participantid} no pathology_record_number')
            # create sample from parent_bems_id
            sample_bems_id = line.get('parent_bems_id', None)
            if not sample_bems_id:
                sample_bems_id = line.get('bems_id', None)
            if not sample_bems_id:
                print(f'source {source} participantid {participantid} no pathology_record_number and no sample_bems_id', file=sys.stderr)
                continue
            sample = {'source': source, 'sample_code': f"BEMS-{sample_bems_id}"}
            sample['submitter_id'] = f"{case['participantid']}/{sample['sample_code']}"
            sample['aliquots'] = []
            # TODO
            # sample['timestamp'] = convert(line.get('date', None))
            # sample['sample_type'] = f"TODO {line.get('sample_type_id', None)}"
            sample['type'] = 'sample'
            case['samples'].append(sample)

        else:
            if 'samples' not in case:
                # print(f'source {source} participantid {participantid} {pathology_record_number} has no samples')
                case['samples'] = []
            # get the sample based on pathology_record_number
            for s in case['samples']:
                if s['sample_code'] == pathology_record_number:
                    sample = s
            # create a sample for the missing pathology_record_number
            if not sample:
                print(f'source {source} participantid {participantid} {pathology_record_number} not found in samples', file=sys.stderr)
                sample = {'source': source, 'sample_code': pathology_record_number}
                sample['submitter_id'] = f"{case['participantid']}/{sample['sample_code']}"
                sample['aliquots'] = []
                # TODO
                # sample['timestamp'] = convert(line.get('date', None))
                # sample['sample_type'] = f"TODO {line.get('sample_type_id', None)}"
                sample['type'] = 'sample'
                case['samples'].append(sample)
        if not sample:
            print(f'source {source} participantid {participantid} {pathology_record_number} {sample_bems_id} no sample', file=sys.stderr)
            continue

        obj = {'submitter_id': f"{line['participantid']}/{line['bems_id']}/{line['pathology_record_number']}/{line['collection_date']}", 'read_groups': []}
        for k in line:
            if aliquot_ignore(k):
                continue
            if 'date' in k.lower():
                obj[k] = convert(line[k])
            else:
                obj[k] = line[k]


        if obj['submitter_id'] in aliquots_ids:
            print(f"duplicate aliquots_ids {obj['submitter_id']}", file=sys.stderr)
            continue
        aliquots_ids[obj['submitter_id']] = True
        sample['aliquots'].append(obj)
        print(json.dumps(obj, separators=(',',':'), default=json_serial))

# done deduping
aliquots_ids = None

# print([(k, v) for k, v in pathology_record_number_counts.items() if v > 1])
