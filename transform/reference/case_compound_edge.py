
from edge import transform, path_to_type, default_parser, existing_node_ids
import uuid

"""
{
  "_id": "(Case:30a1fe5e-5b12-472c-aa86-c2db8167ab23)--TreatedWith->(Compound:CID2662)",
  "gid": "(Case:30a1fe5e-5b12-472c-aa86-c2db8167ab23)--TreatedWith->(Compound:CID2662)",
  "label": "TreatedWith",
  "from": "Case:30a1fe5e-5b12-472c-aa86-c2db8167ab23",
  "to": "Compound:CID2662",
  "data": {}
}

select * from edge_casetreatedwithcompound ;
 created | acl | _sysan | _props | src_id | dst_id
---------+-----+--------+--------+--------+--------

"""

if __name__ == "__main__":
    item_paths = ['source/reference/normalized.TreatedWith.Edge.json.gz']
    type = path_to_type(item_paths[0])
    args = default_parser().parse_args()

    src_node_ids = existing_node_ids('node_case')
    print('src_node_ids', len(src_node_ids.keys()))

    dst_node_ids = existing_node_ids('node_compound')
    print('dst_node_ids', len(dst_node_ids.keys()))


    def ignore_missing(line):
        src_id= uuid.uuid5(uuid.NAMESPACE_DNS, line['from'])
        dst_id = uuid.uuid5(uuid.NAMESPACE_DNS, line['to'])
        if line['from'].lower() not in src_node_ids:
            # print(line['from'].lower(), 'not in src_node_ids')
            print(line['from'])
            # exit()
            return None
        if dst_id not in dst_node_ids:
            print(dst_id, 'not in dst_node_ids')
            return None
        return line

    transform(item_paths, output_dir=args.output_dir, type='edge_casetreatedwithcompound', project_id='smmart-reference', filter=ignore_missing)
