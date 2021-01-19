import csv
import json
import operator

import db_api

ops = {"=": operator.eq, "<": operator.lt, "<=": operator.le, ">": operator.gt, ">=": operator.ge, "!=": operator.ne}


def is_relevant(operat, left, right):
    return ops[operat](left, right)


def filter_record(enum, criteria, row):
    for i in criteria:
        x = int(row[enum[i.field_name]]) if type(i.value) == int else row[enum[i.field_name]]
        if not is_relevant(i.operator, x, i.value):
            return False
    return True


def get_meta_data(table_name):
    with open(db_api.DB_ROOT / 'MetaData.json', "r") as json_file:
        meta_data = json.load(json_file)
        return meta_data[table_name]


def update_meta_data(table_name):
    with open(db_api.DB_ROOT / 'MetaData.json', "r+") as file:
        meta_data = json.load(file)
        meta_data[table_name][1]+=1
        file.seek(0)
        json.dump(meta_data, file)

def get_key_field_name(fields):
    for field in fields:
        if fields[field] == fields['primary key'] and field != 'primary key':
            return field

def get_num_of_lines(table_name,i):
    with open(db_api.DB_ROOT / f'{table_name}_{i}.csv', 'r', newline='') as file:
        csv_reader = csv.reader(file)
        return sum(1 for row in csv_reader)