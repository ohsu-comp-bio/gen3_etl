
import hashlib
import os
import json
import uuid

# make a UUID using a SHA-1 hash of a namespace UUID and a name

from gen3_etl.utils.ioutils import reader

from gen3_etl.utils.defaults import DEFAULT_OUTPUT_DIR, DEFAULT_EXPERIMENT_CODE, default_parser, emitter, path_to_type
from gen3_etl.utils.schema import generate, template

import psycopg2
import psycopg2.extras

def cursor(
    db_host="localhost",
    db_username="sheepdog_user",
    db_password="sheepdog_pass",
    db_database="metadata_db"
):
    conn_string = "host={} dbname={} user={} password={}".format(db_host, db_database, db_username, db_password)
    # print("Connecting to database\n	->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def existing_node_ids(table_name, cursor=cursor()):
    sql = 'SELECT _props->>\'submitter_id\', node_id as "submitter_id" from  "{}";'.format(table_name)
    cursor.execute(sql)
    # retrieve the records from the database
    return {t[0]:t[1] for t in cursor.fetchall()}


def transform(item_paths, output_dir, project_id, type, filter=None):
    """Read bcc labkey json and writes postgres TSV with embedded gen3 json."""
    path = os.path.join(output_dir, '{}.tsv'.format(type))
    dedupes = set([])
    with open(path, 'w') as output_file:
        for p in item_paths:
            for line in reader(p):
                if filter and not filter(line):
                    continue
                src_id = uuid.uuid5(uuid.NAMESPACE_DNS, line['from'].lower())
                dst_id = uuid.uuid5(uuid.NAMESPACE_DNS, line['to'].lower())
                dedupe = '{}-{}'.format(src_id, dst_id)
                if dedupe in dedupes:
                    continue
                line['data']['from'] = line['from']
                line['data']['to'] = line['to']
                # copy $type (src_id, dst_id, acl, _sysan, _props) from stdin  csv delimiter E'\x01' quote E'\x02' ;"
                output_file.write('{}\x01{}\x01{}\x01{}\x01{}\n'.format(src_id, dst_id, '{}', '{}', json.dumps(line['data'], separators=(',',':'))))
                dedupes.add(dedupe)
