"""
test_confParser.py
~~~~~~~~~~~~~~~
This module contains unit tests for the COnfParser.py and methods in ConfParser
"""
import os
import shutil
import unittest

from jobs.ConfParser import ConfParser


class ConfParserTests(unittest.TestCase):

    def test_parse_config(self):
        """
        test if parse_config for yaml
        :return:
        """
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'test.yaml'))
        parsed_conf = conf.parse_config()
        self.assertGreater(len(parsed_conf.keys()), 0)

    def test_parse_config_invalid_case(self):
        """
        test if parse_config for right exception
        :return:
        """

        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'test_invalid.yaml'))
        with self.assertRaises(Exception):
            conf.parse_config()

    def test_parse_config_invalid_input_case(self):
        """
        test if parse_config for right exception
        :return:
        """
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = conf = {"output": {"sql": {"host_name": "some_host"}}}
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing sql section in input
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"input": "test",
                     "output": {
                         "sql": {
                             "host_name": "some_host"
                         }
                     }
                     }
        with self.assertRaises(Exception):
            conf.parse_config()

            # missing query_file_path section in input
            conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
            conf.conf = {"input": {"sql": {}},
                         "output": {
                             "sql": {
                                 "host_name": "some_host"
                             }
                         }
                         }
            with self.assertRaises(Exception):
                conf.parse_config()

        # missing host_name section in input
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"input": {
            "sql": {
                "port": 1234,
                "user_name": "abc",
                "password": "abc123"
            }
        },
            "output": {
                "sql": {
                    "host_name": "some_host"
                }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing port section in input
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"input": {
            "sql": {
                "host_name": "some_host",
                "user_name": "abc",
                "password": "abc123"
            }
        },
            "output": {
                "sql": {
                    "host_name": "some_host"
                }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing user_name section in input
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"input": {
            "sql": {
                "host_name": "some_host",
                "port": 1234,
                "password": "abc123"
            }
        },
            "output": {
                "sql": {
                    "host_name": "some_host"
                }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing password section in input
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"input": {
            "sql": {
                "host_name": "some_host",
                "port": 1234,
                "user_name": "abc123"
            }
        },
            "output": {
                "sql": {
                    "host_name": "some_host"
                }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

    def test_parse_config_invalid_output_sql(self):
        """
        test if parse_config for right exception
        :return:
        """
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        # missing output section
        conf.conf = conf = {"input": {
            "sql": {"host_name": "some_host",
                    "port": 1234,
                    "user_name": "abc",
                    "password": "abc123"
                    }
        }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing sql section in output
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"input": {
            "sql": {"host_name": "some_host",
                    "port": 1234,
                    "user_name": "abc",
                    "password": "abc123"
                    }
        },
            "output": {
                "data_lake": {
                    "output_file_format": "orc"
                }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing host_name section in input
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"output": {
            "sql": {
                "port": 1234,
                "user_name": "abc",
                "password": "abc123"
            }
        },
            "input": {
                "sql": {"host_name": "some_host",
                        "port": 1234,
                        "user_name": "abc",
                        "password": "abc123"
                        }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing port section in input
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"output": {
            "sql": {
                "host_name": "some_host",
                "user_name": "abc",
                "password": "abc123"
            }
        },
            "input": {
                "sql": {"host_name": "some_host",
                        "port": 1234,
                        "user_name": "abc",
                        "password": "abc123"
                        }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing user_name section in output
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"output": {
            "sql": {
                "host_name": "some_host",
                "port": 1234,
                "password": "abc123"
            }
        },
            "input": {
                "sql": {"host_name": "some_host",
                        "port": 1234,
                        "user_name": "abc",
                        "password": "abc123"
                        }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

        # missing password section in output
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
        conf.conf = {"output": {
            "sql": {
                "host_name": "some_host",
                "port": 1234,
                "user_name": "abc123"
            }
        },
            "input": {
                "sql": {"host_name": "some_host",
                        "port": 1234,
                        "user_name": "abc",
                        "password": "abc123"
                        }
            }
        }
        with self.assertRaises(Exception):
            conf.parse_config()

            # missing datalake section in output
            conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'invalid_input.yaml'))
            conf.conf = {"output": {
                "sql": {
                    "host_name": "some_host",
                    "port": 1234,
                    "user_name": "abc123",
                    "password": "abc123"
                }
            },
                "input": {
                    "sql": {"host_name": "some_host",
                            "port": 1234,
                            "user_name": "abc",
                            "password": "abc123"
                            }
                }
            }
            with self.assertRaises(Exception):
                conf.parse_config()

    def test_input_query(self):
        """
        Test to check input_query method
        :return: None
        """
        query = "select * from test"
        # create temp dir for storing query
        if not os.path.exists(os.path.join(os.curdir, 'test_conf')):
            os.mkdir(os.path.join(os.curdir, 'test_conf'))

        path = os.path.join(os.curdir, 'test_conf', 'test_query.sql')
        file = open(path, "w")
        file.write(query)
        file.close()
        conf = ConfParser(conf_file=os.path.join(os.curdir, 'conf', 'test.yaml'))
        # update query_file_path in conf
        conf.parse_config()
        conf.parsed_conf['input']['query_file_path'] = path

        conf_query_output = conf.get_input_query()
        self.assertEqual(query, conf_query_output)
        # remove created dir
        shutil.rmtree(os.path.join(os.curdir, 'test_conf'))


if __name__ == '__main__':
    unittest.main()
