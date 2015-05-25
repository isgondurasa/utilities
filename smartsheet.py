#coding: utf-8

##############################################################
#                   ANDREY SVIRIDOV
#                smartsheet tool

#current features:
# - create new smartsheet in your current workspace +
# - delete selected smartsheet -
# - import from xlsx spreadsheets +-
# - row indent (check)
# - insert formulas (but you should remove front ' from a text cell).
#   if you need to update CHECKBOX cell with formula, you should manually change its type to TEXT,
#   restart script. and then manually turn back its type to CHECKBOX (check)

#TODO:
# 1) filter file extension

##############################################################

import xlrd
import logging
import json
import urllib2
from django.conf import settings
from functools import wraps
from collections import defaultdict
from django.core.management.base import BaseCommand

DEBUG = True

class DuplicateSmartSheet(Exception):
    pass

def stringify(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return json.loads(f(*args, **kwargs))
    return wrapper


class SimpleRestManager(object):

    def __init__(self, client):
        self.client = client
        self.header = "Content-Type", " application/json"

    def raw_put(self, path, data=None):
        u""" method update"""
        return self.client.raw_request(path, self.header, data=data, method="PUT")

    def raw_post(self, path, data=None):
        u""" method create"""
        return self.client.raw_request(path, self.header, data=data, method="POST")

    def raw_get(self, path, attr=None):
        u""" method read"""
        return self.client.raw_request(path.format(attr))

    def raw_delete(self, path):
        u""" method delete"""
        logging.info(path)
        res = self.client.raw_request(path, method="DELETE")
        return res


class SmartsheetAPI(object):
    """ Template for making calls to the Smartsheet API """

    def __init__(self, url, token):
        self.baseURL = url
        self.token = " Bearer " + str(token)

    def raw_request(self, url, extra_header=None, data=None, method=None):
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
            if method == "DELETE":
                req.get_method = lambda: 'DELETE'

        self.resp = urllib2.urlopen(req).read()
        return self.resp


class SmartsheetCmdWrapper(object):
    def __init__(self, url, token):
        client = self.connect_and_get_client(url, token)
        self.rest_helper = SimpleRestManager(client)
        self.sheet = None

    def set_current_sheet_id(self, sheet):
        self.sheet = sheet['id']

    def get_current_sheet(self):
        return self.sheet

    def connect_and_get_client(self, url, token):
        client = SmartsheetAPI(url, token)
        if DEBUG:
            logging.info("Connected to API with token in DEBUG mode")
        else:
            logging.info("Connected to API with token in PRODUCTION mode")
        return client

    @stringify
    def get_sheet(self, sheet_id):
        return self.rest_helper.raw_get('/sheet/{}'.format(sheet_id))

    @stringify
    def get_sheets(self):
        return self.rest_helper.raw_get('/sheets/')

    @stringify
    def get_cols(self, sheet_id):
        cols = self.rest_helper.raw_get('/sheet/{}/columns'.format(TEST_SHEET_ID))
        return cols

    @stringify
    def get_row(self, sheet_id, row_id):
        return self.rest_helper.raw_get('/sheet/{}/row/{}'.format(sheet_id, row_id))

    @stringify
    def get_rows(self, sheet):
        return sheet.get('rows') or -1

    def chunk_rows(self, rows, chunk_size):
        u"""
            chunks rows into several chunks
        """
        for i in xrange(0, len(rows),chunk_size):
            yield rows[i:i+chunk_size]

    @stringify
    def bulk_row_create(self, data, sheet_id):
        path = '/sheet/{}/rows'.format(sheet_id)
        return self.rest_helper.raw_post(path, data=data)

    @stringify
    def bulk_row_update(self, rows, sheet_id):
        path = '/sheet/{}/rows'.format(sheet_id)
        logging.info(rows[2])
        return self.rest_helper.raw_put(path, data=json.dumps({"toBottom":True, "rows":rows}))

    def row_update(self, data, sheet_id, row_id=None):
        path = '/sheet/{}/row/{}'.format(sheet_id, row_id)
        self.rest_helper.raw_put(path, data=data)

    def populate_row(self, row, columns):
        u"""
            returns a list of {"columnId":col__id, "value":row__value}
        """
        col_ids = [str(x['id']) for x in columns]
        populated_row = []
        for index, element in enumerate(row):
            template = {
                "columnId": col_ids[index],
                "value": element
            }
            populated_row.append(template)
        return populated_row

    def get_root_row(self, rows, key_pos):
        return sorted(rows, key=lambda x: x['cells'][key_pos]['value'])[0]

    def sort_rows_by_key(self, rows, key_pos):
        return sorted(rows, key=lambda x: x['cells'][key_pos]['value'])

    def sort_rows_by_path(self, rows):
        return sorted(rows, key=lambda x: x)

    def set_root_in_top(self, rows, key_pos):
        row = sorted(rows, key=lambda x: x['cells'][key_pos]['value'])[0]
        return row

    def fill_with_data(self, sheet, data, key_pos):
        u"""
            {"toTop":True, "rows":[ {"cells": [ {"columnId":column_info[0]['id'], "value":"Brownies"},
                {"columnId":column_info[1]['id'], "value":"julieanne@smartsheet.com","strict": False},
                {"columnId":column_info[2]['id'], "value":"$1", "strict":False},
                {"columnId":column_info[3]['id'], "value":True},
                {"columnId":column_info[4]['id'], "value":"Finished"},
                {"columnId":column_info[5]['id'], "value": "None", "strict":False}]
                },
                . . .
            }
            request may return 404
        """
        rows = []
        for head, row in data.iteritems():
            cells = self.populate_row(row, sheet["result"]["columns"])
            rows.append({"cells": cells})

        root_row = self.get_root_row(rows, key_pos)
        for row in rows:
            if row['cells'][key_pos]['value'] == root_row['cells'][key_pos]['value']:
                row['toTop'] = True
        return rows

    def make_rows_as_dict(self, rows, key_pos=2):
        result = {}
        for row in rows:
            result[row[key_pos]] = row
        return result

    def rec_get_row_path(self, mapping, parent, row_list):
        if parent is None:
            return row_list
        row_list.append(parent)
        children = mapping[parent]

        if not children:
            return row_list
        for child in children:
            self.rec_get_row_path(mapping, child, row_list)
        return row_list

    def divide_rows(self, rows, mapped_rows, root_row):
        u"""
            if len(rows) <= 5000 - then ok
            if len(rows) > 5000 - then we should divide rows
            into several sections.
        """
        #assume we have root row in the top
        #root_row = rows[0]
        root_row_childs = mapped_rows[root_row]
        sheets = {}
        children = []
        index = 1
        for count, root_row_child in enumerate(root_row_childs):
            root_row_child_list = self.rec_get_row_path(mapped_rows,
                                                        root_row_child, [])
            if len(children) + len(root_row_child_list)  < settings.SHEET_CHUNK_SIZE:
                children.extend(root_row_child_list)
            else:
                sheets[index] = list(set(children))
                sheets[index].append(root_row)
                children = root_row_child_list#if len(root_row_child_list) <= settings.SHEET_CHUNK_SIZE
                index += 1
            print count
        else:
            if index not in sheets:
                sheets[index] = list(set(children))
                sheets[index].append(root_row)

        logging.info(sheets)
        return sheets


    @stringify
    def create_smartsheet(self, title, cols=None):
        u"""
            input schema:
            {
                "name":"newsheet",
                "columns":[
                    {
                        "title":"Favorite",
                        "type":"CHECKBOX",
                        "symbol":"STAR"
                    },
                    {
                        "title":"Primary Column",
                        "primary":true,"type":"TEXT_NUMBER"
                    }
                ]
            }
        """
        attrs = {
            "name": title,
            "columns": cols or [{"title": "Path", "type": "TEXT_NUMBER"}]
        }
        attrs = json.dumps(attrs)
        return self.rest_helper.raw_post('/sheets', data=attrs)

    def compare(self, previous, current):
        if len(previous) >= len(current):
            return False

        for parent_row_p_el in previous:
            if parent_row_p_el not in current:
                return False
        return True

    def get_raw_children(self, parent_row, rows, key_pos):
        children = []
        parent_row_path = filter(lambda x: x, parent_row[key_pos].split("/"))
        for row in rows[1:]:
            current_path = filter(lambda x: x, row[key_pos].split("/"))
            if not self.compare(parent_row_path, current_path):
                continue
            diff = row[key_pos].replace(parent_row[key_pos], "")
            if len(filter(lambda x: x, diff.split("/"))) == 1:
                children.append(row[key_pos])

        return children

    def get_children(self, parent_row, rows, key_pos):
        children = []
        get_val = lambda x: x['cells'][key_pos]['value']
        if not parent_row['cells'][0].get('value'):
            return []
        parent_row_path = filter(lambda x: x, parent_row['cells'][key_pos]['value'].split("/"))
        for row in rows[1:]:
            current_path = filter(lambda x: x, row['cells'][key_pos]['value'].split("/"))

            if not self.compare(parent_row_path, current_path):
                continue

            diff = get_val(row).replace(get_val(parent_row), "")

            if len(filter(lambda x: x, diff.split("/"))) == 1:
                children.append(row['id'])
        return children

    def iterate_rows(self, rows, key_pos, raw=None):
        result = {}
        folders = {}
        for row in rows:
            if raw:
                children = self.get_raw_children(row, rows, 2)
            else:
                children = self.get_children(row, rows, key_pos)

            for child in children:
                result[child] = row if raw else row['id']
            if raw:
                folders[row[2]] = children
            else:
                folders[row['id']] = children

        if not raw:
            for row in rows:
                if result.get(row['id']):
                    row['parentId'] = str(result[row['id']])
        return rows, folders

    def get_folders(self, rows, folders):
        excluded_leafs = {}
        for row_id, childs in folders.iteritems():
            if not childs:
                excluded_leafs[row_id] = None
        # search only file folders:
        file_folders = defaultdict(list)
        for leaf, _ in excluded_leafs.iteritems():
            for row_id, childs in folders.iteritems():
                if leaf in childs:
                    file_folders[row_id] += childs
        return file_folders

    def get_col_id_by_name(self, col, sheet):
        for col_in_sheet in sheet['columns']:
            if col_in_sheet['title'].lower() == col.lower().lstrip().rstrip():
                return col_in_sheet['id']

    def set_percentage_marks(self, rows, file_folders, columns, sheet=None):
        cols = [self.get_col_id_by_name(col, sheet) for col in columns.split(",")]
        cols = filter(lambda x: x, cols)
        for row in rows:
            print cols
            if row['id'] in file_folders:
                for col in cols:
                    new_cell = {
                        'columnId': col,
                        'value': settings.SMARTSHEET_FORMULA
                    }
                    data = json.dumps({"cells": [new_cell]})
                    logging.info("UPDATE ROW {} with data {}".format(row['id'], data))
                    try:
                        self.row_update(data=data, sheet_id=sheet['id'], row_id=row['id'])
                    except KeyError as e:
                        logging.exception("There is no sheet in class attr")
                    except Exception as e:
                        logging.exception(e)

    def get_key_id(self, col_name, sheet):
        if sheet.get('result'):
            cols = sheet.get('result').get('columns')
        else:
            cols = sheet.get('columns')

        for index, col in enumerate(cols):
            if col['title'].lower() == col_name.lower():
                return col['id'], index
        return -1, -1

    def remove_smartsheet(self, sheet_id):
        return self.rest_helper.raw_delete('/sheet/{}'.format(sheet_id))

    def handle_excel_file_hierarchy(self, xls_file, key):
        u"""
            imports excel file and returns
            table headers and dict of path: row
        """
        if not xls_file:
            return [], None

        wb = xlrd.open_workbook(file_contents=xls_file, formatting_info=False)
        headers = []
        result = defaultdict(list)
        sheet = wb.sheet_by_index(0)
        key_element = key
        key_element_index = -1
        for rownum in range(sheet.nrows):
            row = sheet.row_values(rownum)
            for index, element in enumerate(row):
                if rownum == 0:
                    headers.append(element)
                    if key_element == element.lower():
                        key_element_index = index
                elif index == key_element_index:
                    result[element] = row
        return headers, result

    def create_smarsheet_cols(self, headers, key_col, additional_cols):
        u"""
            {"columns":[
                        {
                            "title":"Favorite",
                            "type":"CHECKBOX",
                            "symbol":"STAR"
                        },
                        {
                            "title":"Primary Column",
                            "primary":true,"type":"TEXT_NUMBER"
                        }
                    ]
            }
        """
        cols = []
        for header in headers:
            cell = {
                "title": header,
                "type": "TEXT_NUMBER"
            }

            if header.lower() == key_col:
                cell['primary'] = True

            cols.append(cell)

        for col in additional_cols.split(","):
            cell = {
                "title": col,
                "type": "TEXT_NUMBER"
            }
            cols.append(cell)

        return cols


class Command(BaseCommand):

    def add_arguments(self, parser):

        parser.add_argument('new',
                            action="",
                            dest="new_smartsheet",
                            default="",
                            help="Create new smartsheet and name it")

        parser.add_argument('key',
                            action="",
                            dest="key_column",
                            help="Name of the key column",
                            default="path")

        parser.add_argument('file',
                            action="",
                            dest="input_file",
                            default="",
                            help="Input xlsx file from box")

        parser.add_argument('remove',
                            action="",
                            dest="remove_sheet_id",
                            help="Remove sheet with entered id")

        parser.add_argument('indent',
                            action="",
                            dest="indent_sheet_id",
                            help="Indent sheet with entered id")

        parser.add_argument("cols",
                            action="",
                            dest="col_ids",
                            help="col ids")

        parser.add_argument("worker",
                            action="",
                            dest="worker_type",
                            help="worker type")

        parser.add_argument("path",
                            action="",
                            dest="path",
                            help="path")

        parser.add_argument("schema",
                            action="",
                            dest="schema",
                            help="schema")

        parser.add_argument("headers",
                            action="",
                            dest="headers",
                            help="headers")

    def get_requisites(self):
        return settings.SMARTSHEET_BASE_URL, settings.SMARTSHEET_TOKEN

    def create_sheet_worker(self, schema, headers, root_path, title, key_column=None, cols=None):
        smartsheet = SmartsheetCmdWrapper(*self.get_requisites())
        logging.info("CONNECTED OK")
        columns = smartsheet.create_smarsheet_cols(headers, key_column, cols)
        logging.info("CREATE SMARTSHEET AND COLS OK")

        sorted_path_list = smartsheet.sort_rows_by_path(schema.keys())
        logging.info("SORT ROWS OK")

        rows = []
        for path in sorted_path_list:
            rows.append(schema[path])

        rows, folders = smartsheet.iterate_rows(rows, None, raw=True)
        div_sheets = smartsheet.divide_rows(rows, folders, root_path)
        logging.info("DIVIDE ROWS OK {}".format(div_sheets))
        if len(div_sheets) > 0:
            #dict_of_rows = smartsheet.make_rows_as_dict(rows)
            sheets = []
            for pk in div_sheets.keys():
                rows_group = div_sheets[pk]
                logging.info(rows_group)
                full_title = title
                if not len(div_sheets.keys()) == 1:
                    full_title = title + "_pt" + str(pk)

                sheet = smartsheet.create_smartsheet(full_title, columns)
                sheets.append(sheet)
                _, key_pos = smartsheet.get_key_id(key_column, sheet)
                logging.info("KEY: {}".format(schema.keys()[0]))
                logging.info(schema[schema.keys()[0]])

                rows = dict([(row, schema[row]) for row in rows_group])
                rows = smartsheet.fill_with_data(sheet, rows, key_pos)

                smartsheet.bulk_row_create(json.dumps({"toTop": True, "rows": rows}), sheet['result']["id"])

            return sheets
        else:
            logging.info("NO DIVIDE ROWS OK")
            sheet = smartsheet.create_smartsheet(title, columns)
            logging.info("CREATE SHEET OK")
            key_id, key_pos = smartsheet.get_key_id(key_column, sheet)
            smartsheet.bulk_row_create(json.dumps({"toTop": True, "rows": rows}), sheet['result']["id"])
        return [sheet]

    def indent_sheet_worker(self, sheet_id, key_column=None):
        logging.info("START INDENT")
        smartsheet = SmartsheetCmdWrapper(*self.get_requisites())
        sheet = smartsheet.get_sheet(sheet_id)
        _, key_pos = smartsheet.get_key_id(key_column, sheet)
        rows = smartsheet.sort_rows_by_key(sheet['rows'], key_pos)
        rows, _ = smartsheet.iterate_rows(rows, key_pos)
        for row in rows:
            logging.info("UPDATE ROW {}".format(row['id']))
            if row.get("parentId"):
                smartsheet.row_update(json.dumps({"parentId": row["parentId"]}), sheet_id, row['id'])
        logging.info("END INDENT")
        return sheet['id']

    def add_percentage_worker(self, sheet_id, cols, key_column=None):
        smartsheet = SmartsheetCmdWrapper(*self.get_requisites())
        sheet = smartsheet.get_sheet(sheet_id)
        _, key_pos = smartsheet.get_key_id(key_column, sheet)
        rows = smartsheet.sort_rows_by_key(sheet['rows'], key_pos)
        rows, folders = smartsheet.iterate_rows(rows, key_pos)

        folders = smartsheet.get_folders(rows, folders)
        rows = smartsheet.set_percentage_marks(rows, folders, cols, sheet=sheet)

    def remove_sheet_worker(self, sheet_id):
        smartsheet = SmartsheetCmdWrapper(*self.get_requisites())
        return smartsheet.remove_smartsheet(sheet_id)

    def handle(self, *args, **options):
        res = None

        if options['worker_type'] == 0:
            if options.get('new_smartsheet') and options.get('schema'):
                res = self.create_sheet_worker(options['schema'],
                                               options['headers'],
                                               options['path'],
                                               options['new_smartsheet'],
                                               options['key'],
                                               options.get('col_ids'))
                f = open('sheet_id.txt', 'w')
                f.write("{}".format(res))
                f.close()

        if options['worker_type'] == 1:
            if options.get('indent_sheet_id'):
                res = self.indent_sheet_worker(options.get('indent_sheet_id'), options['key'])

        if options['worker_type'] == 2:
            if options.get('col_ids'):
                self.add_percentage_worker(options['indent_sheet_id'],
                                           options['col_ids'],
                                           options.get('key'))

        if options['worker_type'] == 3:
            logging.info("REMOVE SHEET {}".format(options.get('remove_sheet_id')))
            if options.get('remove_sheet_id'):
                logging.info("REMOVE SHEET {}".format(options.get('remove_sheet_id')))
                res = self.remove_sheet_worker(options['remove_sheet_id'])

        return res
