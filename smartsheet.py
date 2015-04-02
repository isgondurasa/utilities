#coding: utf-8
import logging
import json
import urllib2


TEST_SHEET_ID = ''
TOKEN = ""
BASE_URL = "https://api.smartsheet.com/1.1"
SMARTSHEET_URI = ""

def from_json(f):
    def wrapper(*args, **kwargs):
        return json.loads(f(*args, **kwargs))
    return wrapper

class SmartsheetAPI(object):
    """Template for making calls to the Smartsheet API"""
    def __init__(self,url,token):
        self.baseURL = url
        self.token = " Bearer " + str(token)

    def _raw_request(self, url, extra_header = None, data = None, method = None):
        request_url = self.baseURL + url
        req = urllib2.Request(request_url)
        req.add_header("Authorization", self.token)

        if extra_header:
            req.add_header(extra_header[0], extra_header[1])
        if data:
            req.add_data(data)
        if method:
            if method == 'PUT':
                req.get_method = lambda: 'PUT'

        self.resp = urllib2.urlopen(req).read()
        return self.resp

def connect_and_get_client():
    call = SmartsheetAPI(BASE_URL, TOKEN)
    logging.info("Connected to API with token")
    return call

@from_json
def get_sheet(client):
    #sheets = client._raw_request('/sheets/'.format(TEST_SHEET_ID), method="GET")
    #import ipdb; ipdb.set_trace()
    return client._raw_request('/sheet/{}'.format(TEST_SHEET_ID), method="GET")


def indent_row(client, row_id, parent_row_id):
    try:
        a = client._raw_request('/sheet/{}/row/{}'.format(TEST_SHEET_ID, row_id), data=json.dumps({"parentId": parent_row_id}), method="PUT")
        logging.info("INTEND ROW {} TO PARENT {}".format(row_id, parent_row_id))
    except Exception as e:
        logging.exception("ERROR: ROW {} PARENT_ROW {}".format(row_id, parent_row_id))

@from_json
def get_row(client, row_id):
    return client._raw_request('/sheet/{}/row/{}'.format(TEST_SHEET_ID, row_id))

def _get_rows(sheet):
    return sheet['rows']

def get_children(parent_row, rows):
    children = []

    if not parent_row['cells'][0].get('value'):
        return []
    parent_row_path = filter(lambda x: x, parent_row['cells'][0]['value'].split("/"))
    for row in rows[1:]:
        current_path = filter(lambda x: x, row['cells'][0]['value'].split("/"))



        if len(parent_row_path) >= len(current_path):
            continue

        for parent_row_p_el in parent_row_path:
            if parent_row_p_el not in current_path:
                continue

        #import ipdb; ipdb.set_trace()

        diff = row['cells'][0]['value'].replace(parent_row['cells'][0]['value'], "")
        if len(filter(lambda x: x, diff.split("/"))) == 1:
            #import ipdb; ipdb.set_trace()
            children.append(row['id'])
            logging.info("PP AS '{}' and CP as '{}'".format(parent_row['cells'][0]['value'], row['cells'][0]['value']))
            #logging.info("IS CHILD")

        # index = parent_row['cells'][0]['value'].find(row['cells'][0]['value'])
        # if index >=0:
        #     difference_path = row['cells'][0]['value'][len(parent_row['cells'][0]['value']):]
        #     import ipdb; ipdb.set_trace()
        #     if filter(lambda x: x, difference_path.split("/")) == 1:
        #         children.append(row['id'])
        #         logging.info("IS CHILD")

        # if len(parent_row_path) - len(current_path) == 1:
        #     c_path = set(current_path)
        #     p_path = set(parent_row_path)
        #     l_path = list(p_path.difference(c_path))
        #     logging.info(l_path)
        #     if len(l_path) == 1:
        #         if row['id'] != parent_row['id']:
        #             children.append(row['id'])
        #             logging.info("IS CHILD")
    return children


def iterate_rows(client, rows, parent_path=None, parent_row_id=None):
    result = {}
    for row in rows:
        #logging.info("WORKING WITH {} ROW".format(row['id']))
        result[row['id']] = get_children(row, rows)
        #logging.info("FOUND {} CHILDREN".format(len(result[row['id']])))


    for row_id, childs in result.iteritems():
        logging.info("ROW {}, CHILDREN: {}".format(row_id, childs))
        for child in childs:
            indent_row(client, child, row_id)


def run():
    logging.basicConfig(filename="smartsheet.log", level=logging.DEBUG)
    client = connect_and_get_client()

    sheet = get_sheet(client)

    rows = _get_rows(sheet)

    iterate_rows(client, rows)
    logging.info('DONE')
    #import ipdb; ipdb.set_trace()


if __name__ == "__main__":
    run()


            # current_path = row['cells'][0]['value']
        # current_path_ = set(row['cells'][0]['value'].split("/"))
        # if not parent_path:
        #     iterate_rows(client, rows[1:], current_path, row['id'])
        # parent_path_ = set(parent_path.split("/"))

        # if len(list(parent_path_.difference(current_path))) > 0:
        #     if parent_row_id != row['id']:
        #         indent_row(client, row['id'], parent_row_id)
        #         parent_row_id = row['id']
        #         iterate_rows(client, rows[1:], current_path, row['id'])
