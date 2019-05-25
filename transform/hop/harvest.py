import requests
import os
import csv
import zipfile
import json
from defaults import DEFAULT_INPUT_FILE


class MedableConnection:

    def __init__(self):
        """Configures connection to medable, connects."""
        with open('config/medable.json', 'r') as config_file:
             config = json.load(config_file)
        self.api_key = config['api_key']
        self.credentials = config['credentials']
        self.base_url = config['base_url']
        # make call with http headers
        self.headers = {'medable-client-key': self.api_key}
        self.cookies = {}
        url = '{}/accounts/login'.format(self.base_url)
        response = requests.post(url,
                                 headers=self.headers,
                                 cookies=self.cookies,
                                 data=self.credentials)
        assert response.status_code == 200 , 'should have returned a 200 response {}'.format(response.json())
        # pass session cookie to future responses
        self.cookies = response.cookies
        # print('login successful')

    def get_meta_data(self):
        """Harvests meta data."""
        data = {}
        for object_name in ['accounts','org','signature', 'objects', 'notifications', 'connections', 'exports', 'logs']:
            url = '{}/{}'.format(self.base_url, object_name)
            response = requests.get(url, headers=headers, cookies=cookies)
            # assert all is ok
            assert response.status_code == 200 , 'should have returned a 200 response {}'.format(response.json())
            data[object_name] = response.json()
            # print(url, len(data[object_name]['data']))
        self.meta_data = data
        # custom objects
        objects = {}
        for d in data['objects']['data']:
           objects[d['name']] = {
               '_id': d['_id'],
               'label': d['label'],
               'properties': d['properties'],
               'pluralName': d['pluralName']
           }
           objects[d['name']]['properties'].append(['c_value', 'c_data', 'c_file'])
        self.custom_objects = objects
        # for k,v in objects.items():
        #     print(k, v['_id'], len(v['properties']) )

    def download_file(self, url, local_filename):
        """Downloads url to local_filename."""
        # NOTE the stream=True parameter below
        with requests.get(url, cookies=self.cookies,
                          headers=self.headers, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
                        # f.flush()
        return local_filename


    def create_export(self, object_name):
        """Creates a medable export."""
        object_name_plural = objects[object_name]['pluralName']
        export = {
            "label": '{}-export'.format(object_name_plural),
            "format": "application/x-ndjson",
            "objects": object_name_plural,
            "paths": [p['name'] for p in objects[object_name]['properties']],
            "afterScript": "import logger from 'logger';\nlogger.info(script.arguments.err || script.arguments.export);\n"
        }
        url = '{}/exports'.format(self.base_url)
        response = requests.post(url, cookies=self.cookies,
                                 headers=self.headers,  json=export)
        assert response.status_code == 200 , 'should have returned a 200 response {}'.format(response.json())
        return response.json()


    def get_export(self, export_id=None, export_label=None):
        """Gets export record."""
        if export_id:
            url = '{}/exports/{}'.format(self.base_url,export_id)
            response = requests.get(url, cookies=self.cookies,
                                    headers=self.headers)
            assert response.status_code == 200 , 'should have returned a 200 response {}'.format(response.json())
            return response.json()
        if export_label:
            url = '{}/exports'.format(self.base_url,export_id)
            response = requests.get(url, cookies=self.cookies,
                                    headers=self.headers)
            assert response.status_code == 200 , 'should have returned a 200 response {}'.format(response.json())
            data = response.json()['data']
            for export in data:
                if export['label'] == export_label:
                    return export
            raise Exception('{} not found'.format(export_label))
        raise Exception('need export_id or export_label parameter')




    def export_ready(self, export_id):
        """Retrieves state ."""
        export_state = self.get_export(export_id)
        if export_state['state'] == 'ready':
            return object_name
        return None


    def download_export(self, source_dir, export_response):
        export_id = export_response['_id']
        url = '{}/exports/{}'.format(self.base_url,export_id)
        response = requests.get(url, headers=self.headers, cookies=self.cookies)
        assert response.status_code == 200 , 'should have returned a 200 response {}'.format(response.json())
        export_state = response.json()
        if export_state['state'] == 'ready':
            url = '{}{}'.format(self.base_url, export_state['dataFile']['path'])
            local_filename = '{}/{}.zip'.format(source_dir, export_state['dataFile']['filename'])
            os.makedirs(source_dir, exist_ok=True)
            print('downloading {}'.format(url))
            self.download_file(url, local_filename)
            # print('downloaded {}'.format(local_filename))
            return local_filename
        else:
            print('not ready for download ({})'.format(export_state['state']))
            return None




# exports = {}
# for object_name in objects:
#     try:
#         exports[object_name] = create_export(object_name, cookies, headers)
#     except Exception as e:
#         print('could not create export for {} {}'.format(object_name, e))
#
# downloaded = []
# for object_name, export_response in exports.items():
#     export_id = export_response['_id']
#     if export_id in downloaded:
#         continue
#     print('checking {}'.format(object_name))
#     if export_ready(export_id, cookies, headers):
#         download_export(object_name, export_response, cookies, headers)
#         downloaded.append(export_id)


connection = MedableConnection()


# export_state = connection.get_export(export_id='5cdeb581e0554c01009e6cf7')

export_state = connection.get_export(export_label='healthy_oregon_project_hop_responses')


path_to_zip_file = connection.download_export('source/hop', export_state)
if path_to_zip_file:
    print('unzipping {}'.format(path_to_zip_file))
    directory_to_extract_to = 'source/hop'
    zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
    zip_ref.extractall(directory_to_extract_to)
    zip_ref.close()
    unzipped_file = zip_ref.filename.replace('.zip','')
    assert os.path.isfile(unzipped_file), 'file should exist {}'.format(unzipped_file)
    os.rename(unzipped_file, DEFAULT_INPUT_FILE)
    assert os.path.isfile(DEFAULT_INPUT_FILE), 'file should exist {}'.format(DEFAULT_INPUT_FILE)
    print('ready {}'.format(DEFAULT_INPUT_FILE))
