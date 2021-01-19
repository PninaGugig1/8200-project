import csv
import json
from typing import List, Any, Dict
import db_api
import os  # for deleting file

from Auxiliary_functions import filter_record, get_meta_data, get_key_field_name, get_num_of_lines, update_meta_data

num_of_lines_in_file = 10


class DBTable(db_api.DBTable):

    def count(self) -> int:
        count = 0
        num_of_files = get_meta_data(self.name)[1]
        for i in range(1, num_of_files + 1):
                count += get_num_of_lines(self.name,i)
        return count - 1

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.get_record(values[self.key_field_name]):
            raise ValueError("Key Already Exists")
        num_of_files = get_meta_data(self.name)[1]
        with open(db_api.DB_ROOT / f'{self.name}_{num_of_files}.csv', 'a', newline='') as file:
            if get_num_of_lines(self.name,num_of_files) == num_of_lines_in_file:
                # create_file(self.name,num_of_files+1)
                num_of_files += 1
                with open(db_api.DB_ROOT / f'{self.name}_{num_of_files}.csv', 'a', newline='') as new_file:
                    csv_writer = csv.writer(new_file)
                    list_ = []
                    for field in self.fields:
                        list_.append(values.get(field)) if type(field) == str else list_.append(values.get(field.name))
                    csv_writer.writerow(list_)
                    update_meta_data(self.name)
            else:
                csv_writer = csv.writer(file)
                list_ = []
                for field in self.fields:
                    list_.append(values.get(field)) if type(field) == str else list_.append(values.get(field.name))
                csv_writer.writerow(list_)

    def delete_record(self, key: Any) -> None:
        if self.get_record(key) is None:
            raise ValueError("Key Does Not Exists")
        meta_data = get_meta_data(self.name)
        index_enum = meta_data[0]
        num_of_files = meta_data[1]
        for j in range(1, num_of_files + 1):
            with open(db_api.DB_ROOT / f'{self.name}_{j}.csv', 'r') as inp, open('n.csv', 'w', newline='') as out:
                writer = csv.writer(out)
                for i, row in enumerate(csv.reader(inp)):
                    if not i:
                        writer.writerow(row)
                    elif row:
                        if row[index_enum['primary key']] != str(key):
                            writer.writerow(row)
            os.remove(f'db_files/{self.name}_{j}.csv')
            os.rename('n.csv', f'db_files/{self.name}_{j}.csv')

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        meta_data = get_meta_data(self.name)
        index_enum = meta_data[0]
        num_of_files = meta_data[1]
        for j in range(1,num_of_files+1):
            with open(db_api.DB_ROOT / f'{self.name}_{j}.csv', 'r') as inp, open('n.csv', 'w', newline='') as out:
                writer = csv.writer(out)
                for i, row in enumerate(csv.reader(inp)):
                    if not i:
                        writer.writerow(row)
                    elif row:
                        if not filter_record(index_enum, criteria, row):
                            writer.writerow(row)
            os.remove(f'db_files/{self.name}_{j}.csv')
            os.rename('n.csv', f'db_files/{self.name}_{j}.csv')

    def get_record(self, key: Any) -> Dict[str, Any]:
        meta_data = get_meta_data(self.name)
        index_enum = meta_data[0]
        num_of_files = meta_data[1]
        list_ = []
        dict_ = {}
        for j in range(1,num_of_files+1):
            with open(db_api.DB_ROOT / f'{self.name}_{j}.csv', 'r') as f:
                for i, row in enumerate(csv.reader(f)):
                    if i == 0:
                        list_ = row
                    elif row:
                        if row[index_enum['primary key']] == str(key):
                            for key2, value in zip(list_, row):
                                dict_[key2] = value
                            return dict_

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        dict_ = self.get_record(str(key))
        for i in dict_.keys():
            if i in values:
                dict_[i] = values[i]
        self.delete_record(key)
        self.insert_record(dict_)

    def query_table(self, criteria: List[db_api.SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        meta_data = get_meta_data(self.name)
        index_enum = meta_data[0]
        num_of_files = meta_data[1]
        list_ = []
        for j in range(1,num_of_files+1):
            with open(db_api.DB_ROOT / f'{self.name}_{j}.csv', 'r') as file:
                for i, row in enumerate(csv.reader(file)):
                    if i:
                        if row:
                            if filter_record(index_enum, criteria, row):
                                list_.append({criteria[0].field_name: row[index_enum[criteria[0].field_name]]})
        return list_

    def create_index(self, field_to_index: str) -> None:
        index_dict = {}
        print(self.get_record(field_to_index))


class DataBase:
    def __init__(self):
        # os.mkdir("db_files")
        if os.path.isfile(db_api.DB_ROOT / "MetaData.json") and os.access('./db_files/MetaData.json', os.R_OK):
            print("File exists and is readable/there")

        else:
            with open(db_api.DB_ROOT / "MetaData.json", 'w') as DB_file:
                json.dump({}, DB_file)

    def create_table(self, table_name: str, fields: List[db_api.DBField], key_field_name: str) -> db_api.DBTable:
        with open(db_api.DB_ROOT / 'MetaData.json', 'r') as file:
            meta_data = json.load(file)
        if meta_data.get(table_name):
            raise ValueError('Table Already Exists')

        with open(db_api.DB_ROOT / f'{table_name}_1.csv', "w", newline='') as file:
            writer = csv.writer(file, delimiter=',')
            list_ = [i.name for i in fields]
            writer.writerow(list_)
        dict_ = {}
        for i, field in enumerate(fields):
            if field.name == key_field_name:
                dict_["primary key"] = i
            dict_[field.name] = i

        with open(db_api.DB_ROOT / 'MetaData.json', "w") as json_file:
            meta_data[table_name] = [dict_, 1]
            json.dump(meta_data, json_file)

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        the_file = open(db_api.DB_ROOT / 'MetaData.json')
        meta_data = json.load(the_file)
        return len(meta_data)

    def get_table(self, table_name: str) -> DBTable:
        with open(db_api.DB_ROOT / 'MetaData.json') as file:
            meta_data = json.load(file)
            if not meta_data.get(table_name):
                raise FileNotFoundError('Table Does Not Exists')
        key_field_name = get_key_field_name(meta_data[table_name][0])
        with open(db_api.DB_ROOT / f"{table_name}_1.csv", "r+") as file:
            for i, row in enumerate(csv.reader(file)):
                if not i:
                    return DBTable(table_name, row, key_field_name)

    def delete_table(self, table_name: str) -> None:
        with open(db_api.DB_ROOT / 'MetaData.json') as file:
            meta_data = json.load(file)
            num_of_lines = meta_data[table_name][1]
            if meta_data.pop(table_name, None) is None:
                raise FileNotFoundError('Table Does Not Exists')
            for i in range(1,num_of_lines+1):
                os.remove(f'db_files/{table_name}_{i}.csv')

        with open(db_api.DB_ROOT / 'MetaData.json', 'w') as data_file:
            json.dump(meta_data, data_file)

    def get_tables_names(self) -> List[Any]:
        table_names = []
        the_file = open(db_api.DB_ROOT / 'MetaData.json')
        meta_data = json.load(the_file)
        for table_name in meta_data:
            table_names.append(table_name)
        return table_names

    # def query_multiple_tables(self, tables: List[str], fields_and_values_list: List[List[db_api.SelectionCriteria]], fields_to_join_by: List[str]
    #                           ) -> List[Dict[str, Any]]:
