"""Parses directory output."""
import re

# https://s3.amazonaws.com/dictionary-artifacts/anvil/master/schema.json

from collections import defaultdict
uri_keys = {
    8: """-/Subject.id/Practioner.id/Task.id/Sample.bmes_id/Sample.id/DocumentReference.data_type/DocumentReference.file_name""".split('/'),
    9: """-/Subject.id/Practioner.id/Task.id/Sample.bmes_id/Sample.id/DocumentReference.data_type/DocumentReference.path/DocumentReference.file_name""".split('/'),
    7: """-/Subject.id/Practioner.id/Task.id/Sample.bmes_id/Sample.id/DocumentReference.file_name""".split('/'),
    6: """-/Subject.id/Practioner.id/Task.id/Sample.bmes_id/DocumentReference.file_name""".split('/'),
}

uri_key_lengths = defaultdict(int)
last_uri_keys = {}

keys = """- - - - DocumentReference.size DocumentReference.last_modified_date DocumentReference.last_modified_time uri""".split()

# unique_keys = defaultdict(set)

def get_atlas_files():
    """Parse directory listing."""
    # '-rwxrwx--- 1 creason  omerosmb  102815802 2019-12-06 09:18 ./HTA9_1/OHSU-RIESTERER/FIB-SEM/BEMS259362/HTA9_1_8/HTA9_1_8_3DEM/HTA9_1_8_3DEM movie.mpg\n'
    with open("source/htan/OMSAtlas-files.txt") as listing:
        pattern = re.compile(r'(?P<permissions>\S+)\s(?P<x>\S)\s(?P<owner>\S+)\s+(?P<group>\S+)\s+(?P<size>\S+)\s(?P<date>\S+)\s(?P<time>\S+)\s(?P<uri>.*$)')
        for line in listing.readlines():
            o = re.match(pattern, line).groupdict()
            o['size'] = int(o['size'])
            uri_parts = o['uri'].split('/')
            if "." not in uri_parts[-1]:
                print(f"uri_parts[-1] should have '.' {uri_parts}   {line}")
                continue

            # profile
            uri_key_lengths[len(uri_parts)] += 1
            last_uri_keys[len(uri_parts)] = o['uri']

            # _uri_keys = uri_keys.get(len(uri_parts), None)
            # if not _uri_keys:
            #     # print("No mapping for", len(uri_parts), o['uri'])
            #     continue

            _uri_keys = """-/Subject.id/Practioner.id/Task.id/Sample.bmes_id/Sample.id""".split('/')
            if len(uri_parts) < len(_uri_keys):
                print(f"missing required key {o['uri']} {line} {len(uri_parts)}")
                continue

            uri = {_uri_keys[i]:uri_parts[i] for i in range(len(_uri_keys))}
            keys_to_delete = []
            if 'CSB08' in uri['Sample.bmes_id']:
                uri['Sample.id'] = uri['Sample.bmes_id']
                keys_to_delete.append('Sample.bmes_id')
            if 'CST02' in uri['Sample.bmes_id']:
                uri['Sample.id'] = uri['Sample.bmes_id']
                keys_to_delete.append('Sample.bmes_id')
            if '.' in uri['Sample.id']:
                keys_to_delete.append('Sample.id')
            for k in keys_to_delete:
                del uri[k]

            keys_to_delete = []
            o['DocumentReference.data_category'] = uri_parts[-2]        
            if 'BEMS' in o['DocumentReference.data_category']:
                keys_to_delete.append('DocumentReference.data_category')
            if 'Sample' in o['DocumentReference.data_category']:
                keys_to_delete.append('DocumentReference.data_category')
            if '@' in o['DocumentReference.data_category']:
                keys_to_delete.append('DocumentReference.data_category')
            if uri.get('Sample.id', None) == o['DocumentReference.data_category']:
                keys_to_delete.append('DocumentReference.data_category')
            if uri.get('Sample.bmes_id', None) == o['DocumentReference.data_category']:
                keys_to_delete.append('DocumentReference.data_category')
            if uri.get('Subject.id', None) in o['DocumentReference.data_category']:
                keys_to_delete.append('DocumentReference.data_category')
            if 'HTA' in o['DocumentReference.data_category']:
                keys_to_delete.append('DocumentReference.data_category')

            for k in set(keys_to_delete):
                del o[k]

            uri['DocumentReference.file_name'] = uri_parts[-1]
            assert "." in uri['DocumentReference.file_name'], f"DocumentReference.file_name should have '.' {uri_parts}   {line}"

            o.update(uri)
            yield o

            # for k,v in o.items():
            #     unique_keys[k].add(v)

            # if 'HTA' in o.get('DocumentReference.data_type', []):
            #     print('htan ?', o['uri'])
            #     break

# print(uri_key_lengths)
# print(last_uri_keys)


# for k in "/Subject.id/Practioner.id/Task.id/Sample.bmes_id/Sample.id/DocumentReference.data_category".split('/'):
#     print(k, unique_keys[k])

# # for k in "DocumentReference.file_name".split('/'):
# #     print(k, unique_keys[k])

# print(f"DocumentReference.file_name {len(unique_keys['DocumentReference.file_name'])}")
# # for v in unique_keys["DocumentReference.file_name"]:
# #     assert "." in v, f"DocumentReference.file_name should have '.' {v}"
