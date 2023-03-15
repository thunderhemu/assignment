import collections
import os

import yaml


class ConfParser:
    def __init__(self, conf_file):
        self.parsed_conf = {}
        self.conf = conf_file

    def parse_config(self):
        """
        parses input conf yaml
        :return: dict
        """
        if os.path.isfile(self.conf):
            with open(self.conf, "r") as stream:
                try:
                    config = yaml.safe_load(stream)
                    self.parsed_conf = config
                    self.validate_conf()
                    return config
                except yaml.YAMLError as exc:
                    print(exc)
        else:
            raise Exception("Invalid Config file!, Config files doesn't exists")

    def get_input_query(self):
        """
        parses conf yaml and returns input query
        :return: Str
        """
        if os.path.isfile(self.parsed_conf['input']['query_file_path']):
            with open(self.parsed_conf['input']['query_file_path'], 'r') as file:
                data = file.read()
            return str(data).replace('[dbo].[table_name]', self.parsed_conf['input']['query_file_path'])
        else:
            raise Exception("Invalid query_file_path! query_file_path doesn't exists ")

    def validate_conf(self):
        if 'input' not in self.parsed_conf.keys():
            raise Exception("Invalid conf! input section doesn't exists in conf")

        if 'sql' not in self.parsed_conf['input'].keys():
            raise Exception("Invalid conf! input -> sql section doesn't exists in conf")

        if 'query_file_path' not in self.parsed_conf['input'].keys():
            raise Exception("Invalid conf! input -> query_file_path section doesn't exists in conf")

        if not \
                collections.Counter(['host_name', 'port', 'users_name', 'password', 'db_name', 'table_name']) == \
                collections.Counter(list(self.parsed_conf['input']['sql'].keys())):
            raise Exception("Invalid conf! input -> sql section doesn't contain required elements")

        if 'output' not in self.parsed_conf.keys():
            raise Exception("Invalid conf! output section doesn't exists in conf")

        if 'sql' not in self.parsed_conf['output'].keys():
            raise Exception("Invalid conf! output -> sql section doesn't exists in conf")

        if not \
                collections.Counter(['host_name', 'port', 'users_name', 'password', 'db_name', 'table_name']) == \
                collections.Counter(list(self.parsed_conf['output']['sql'].keys())):
            raise Exception("Invalid conf! output -> sql section doesn't contain required elements")

        if 'datalake' not in self.parsed_conf['output'].keys():
            raise Exception("Invalid conf!, output -> datalake section")

        if not \
                collections.Counter(['output_file_path', 'output_file_format']) == \
                collections.Counter(list(self.parsed_conf['output']['datalake'].keys())):
            raise Exception("Invalid conf! output -> datalake section doesn't contain required elements")
