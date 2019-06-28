from gen3_etl.utils.cli import default_argument_parser
from gen3_etl.utils.ioutils import JSONEmitter
from gen3_etl.utils.models import default_case, default_sample, default_aliquot, default_diagnosis, default_treatment, default_observation
from gen3_etl.utils.ioutils import reader

import os
import hashlib
import json
from datetime import date, datetime


# all dates have this format
DATE_FORMAT = '%Y/%m/%d %H:%M:%S'


DEFAULT_OUTPUT_DIR = 'output/bcc'
DEFAULT_EXPERIMENT_CODE = 'bcc'
DEFAULT_PROJECT_ID = 'ohsu-bcc'

def emitter(type=None, output_dir=DEFAULT_OUTPUT_DIR, **kwargs):
    """Creates a default emitter for type."""
    return JSONEmitter(os.path.join(output_dir, '{}.json'.format(type)), compresslevel=0, **kwargs)


def default_parser(output_dir, experiment_code, project_id):
    parser = default_argument_parser(
        output_dir=output_dir,
        description='Reads bcc json and writes gen3 json ({}).'.format(output_dir)
    )
    parser.add_argument('--experiment_code', type=str,
                        default=experiment_code,
                        help='Name of gen3 experiment ({}).'.format(experiment_code))
    parser.add_argument('--project_id', type=str,
                        default=project_id,
                        help='Name of gen3 program-project ({}).'.format(project_id))
    parser.add_argument('--schema', type=bool,
                        default=True,
                        help='generate schemas (true).'.format(experiment_code))
    return parser


def missing_parent(**kwargs):
    """Returns dict of missing item."""
    for p in ['parent_id', 'parent_type', 'child_id', 'child_type']:
        assert p in kwargs, 'missing {}'.format(p)
    return kwargs

def save_missing_parents(missing_parents, output_dir=DEFAULT_OUTPUT_DIR):
    """Saves list of missing item. Dedupes."""
    with open('{}/missing.json'.format(output_dir), 'a') as missing_parent_file:
        dedupe = set([])
        for mp in missing_parents:
            js = json.dumps(mp, separators=(',',':'))
            check_sum = hashlib.sha256(js.encode('utf-8')).digest()
            if check_sum in dedupe:
                continue
            dedupe.add(check_sum)
            missing_parent_file.write(js)
            missing_parent_file.write('\n')


DOBs = None

def _DOBs(output_dir):
    global DOBs
    if DOBs:
        return DOBs
    # load date of birth cache
    DOBs = {}
    bcc_participant_path = '{}/bcc_participant_dob.json'.format(output_dir)
    if os.path.isfile(bcc_participant_path):
        for line in reader(bcc_participant_path):
            DOBs[line['participantid']] = datetime.strptime(line['DateOfBirth'], DATE_FORMAT)
    return DOBs


def obscure_dates(line, participantid=None, skip_properties=[], callback=None, output_dir=DEFAULT_OUTPUT_DIR):
    """Replaces all dates with days since birth value."""
    # if participantid not passed, find it
    if not participantid:
        participantid = line.get('participantid', line.get('ParticipantID', None))
    assert participantid, 'Should have a participantid {}'.format(line.keys())
    # get date of birth
    dob = _DOBs(output_dir).get(participantid)
    if not dob:
        return line
    for k,v in line.items():
        if k in skip_properties:
            continue

        if 'date' not in k.lower():
            continue

        # if we can detect a date value, convert it
        delta_v = None
        try:
            date_v = datetime.strptime(v, DATE_FORMAT)
            delta_v = date_v - dob
        except Exception as e:
            pass
        # and replace it
        date_k = k.replace('date', 'days').replace('Date', 'Days')
        skip = False
        if callback:
            # if callback returns T, use default replace
            skip = callback(line, k, v, date_k, delta_v)
        days = None
        if delta_v:
            days = delta_v.days
        if not skip:
            line[date_k] = days
            del line[k]
    return line
