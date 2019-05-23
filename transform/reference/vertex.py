
import hashlib
import os
import json
import uuid

# make a UUID using a SHA-1 hash of a namespace UUID and a name

from gen3_etl.utils.ioutils import reader

from defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, default_parser, emitter, path_to_type
from gen3_etl.utils.schema import generate, template
import os
import re


def transform(item_paths, output_dir, project_id, type, callback=None, compresslevel=0):
    """Read bcc labkey json and writes postgres TSV with embedded gen3 json."""
    path = os.path.join(output_dir, '{}.tsv'.format(type))
    node_ids = set([])
    with open(path, 'w') as output_file:
        for p in item_paths:
            for line in reader(p):
                node_id = uuid.uuid5(uuid.NAMESPACE_DNS, line['gid'].lower())
                if node_id in node_ids:
                    continue
                node_ids.add(node_id)
                line['data']['project_id'] = project_id
                line['data']['submitter_id'] = line['gid'].lower()
                line['node_id'] = node_id
                if callback:
                    line = callback(line)
                # copy node_gene(node_id, acl, _sysan,  _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"
                output_file.write('{}\x01{}\x01{}\x01{}\n'.format(node_id, '{}', '{}', json.dumps(line['data'], separators=(',',':'))))
