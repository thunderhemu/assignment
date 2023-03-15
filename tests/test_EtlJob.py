import os.path
import shutil
import unittest

import pymssql
from pyspark import SparkConf
from pyspark.sql import SparkSession
from testcontainers.mssql import SqlServerContainer
from testcontainers.postgres import PostgresContainer

from jobs import logging
from jobs.etl_job import EtlJob, process_etl
import psycopg2
import wget


class EtlJobTests(unittest.TestCase):
    """Test suite for transformation in EtlJob
        """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        # https://jdbc.postgresql.org/download/postgresql-42.5.4.jar
        postgres_jar_url = "https://jdbc.postgresql.org/download/postgresql-42.5.4.jar"
        mssql_jar = "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/11.2.3.jre18/mssql-jdbc-11.2.3.jre18.jar"
        path = os.path.join(os.getcwd(), 'jars', 'postgresql-42.5.4.jar')
        ms_sql_path = os.path.join(os.getcwd(), 'jars', 'mssql-jdbc-11.2.3.jre18.jar')

        if not os.path.exists(os.path.join(os.getcwd(), 'jars')):
            os.mkdir(os.path.join(os.getcwd(), 'jars'))
            wget.download(postgres_jar_url, path)
            wget.download(mssql_jar, ms_sql_path)

        self.spark = SparkSession.builder.appName("test_etl_job") \
            .config("spark.jars", f'{ms_sql_path}, {path}')\
            .config("spark.driver.extraClassPath", ms_sql_path)\
            .getOrCreate()

    def tearDown(self):
        """Stop Spark and remove the output files created
        """
        self.spark.stop()
        shutil.rmtree(os.path.join(os.curdir, 'jars'))

    def test_write_date_csv(self):
        """
        test if write_data method can write to csv
        :return: None
        """
        columns = ["language", "users_count"]
        data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
        rdd = self.spark.sparkContext.parallelize(data)
        df = rdd.toDF(columns)
        output_path = os.path.join(os.curdir, 'output', 'test_file.csv')
        data_lake = {"output_file_format": "csv", "output_file_path": output_path}
        conf = {"output": {"datalake": data_lake}}
        spark_logger = logging.Log4j(self.spark)
        etl = EtlJob(spark=self.spark, conf=conf, logger=spark_logger)
        etl.write_date(df=df)
        output_df = self.spark.read.option("header", True).csv(output_path)
        self.assertEqual(df.count(), output_df.count())
        self.assertEqual(df.exceptAll(output_df).count(), 0)
        self.assertEqual(output_df.exceptAll(df).count(), 0)
        shutil.rmtree(os.path.join(os.curdir, 'output'))

    def test_write_date_parquet(self):
        """
        test if write_data method can write to parquet
        :return: None
                """
        columns = ["language", "users_count"]
        data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
        rdd = self.spark.sparkContext.parallelize(data)
        df = rdd.toDF(columns)
        output_path = os.path.join(os.curdir, 'output', 'test_file_parquet')
        data_lake = {"output_file_format": "parquet", "output_file_path": output_path}
        conf = {"output": {"datalake": data_lake}}
        spark_logger = logging.Log4j(self.spark)
        etl = EtlJob(spark=self.spark, conf=conf, logger=spark_logger)
        etl.write_date(df=df)
        output_df = self.spark.read.parquet(output_path)
        self.assertEqual(df.count(), output_df.count())
        self.assertEqual(df.exceptAll(output_df).count(), 0)
        self.assertEqual(output_df.exceptAll(df).count(), 0)
        shutil.rmtree(os.path.join(os.curdir, 'output'))

    def test_write_date_orc(self):
        """
        test if write_data method can write to orc
        :return: None
                """
        columns = ["language", "users_count"]
        data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
        rdd = self.spark.sparkContext.parallelize(data)
        df = rdd.toDF(columns)
        output_path = os.path.join(os.curdir, 'output', 'test_file_orc')
        data_lake = {"output_file_format": "orc", "output_file_path": output_path}
        conf = {"output": {"datalake": data_lake}}
        spark_logger = logging.Log4j(self.spark)
        etl = EtlJob(spark=self.spark, conf=conf, logger=spark_logger)
        etl.write_date(df=df)
        output_df = self.spark.read.orc(output_path)
        self.assertEqual(df.count(), output_df.count())
        self.assertEqual(df.exceptAll(output_df).count(), 0)
        self.assertEqual(output_df.exceptAll(df).count(), 0)
        shutil.rmtree(os.path.join(os.curdir, 'output'))

    def test_write_to_rdbms(self):
        spark_logger = logging.Log4j(self.spark)
        postgres_container = PostgresContainer("postgres:9.5")
        user = postgres_container.POSTGRES_USER
        password = postgres_container.POSTGRES_PASSWORD
        postgress_db = postgres_container.POSTGRES_DB
        columns = ["language", "users_count"]
        data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
        rdd = self.spark.sparkContext.parallelize(data)
        df = rdd.toDF(columns)
        conf = {"input": {
            "sql": {
                "host_name": "localhost",
                "port": 5432,
                "db_name": postgress_db,
                "use_name": user,
                "password": password,
                "table_name": "test_input"
            }
        },
            "output": {
                "sql": {
                    "host_name": "localhost",
                    "port": 5432,
                    "db_name": postgress_db,
                    "user_name": user,
                    "password": password,
                    "table_name": "test_output"
                }
            }
        }
        etl = EtlJob(spark=self.spark, conf=conf, logger=spark_logger)
        with postgres_container.with_bind_ports(5432, 5432) as postgres:
            print(postgres.get_connection_url())
            conn = psycopg2.connect(
                host="localhost",
                database=postgress_db,
                user="test",
                port=5432,
                password="test")
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute('CREATE SCHEMA test ;')
            cur.execute('CREATE TABLE  test.test_output(language VARCHAR, users_count VARCHAR) ;')
            cur.close()
            etl.write_to_rdbms(df=df)
            df_out = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://localhost:5432/test") \
                .option("driver", "org.postgresql.Driver").option("dbtable", "test.test_output") \
                .option("user", user).option("password", password).load()
            df.show()
            df_out.show()

    def test_process_etl(self):
        spark_logger = logging.Log4j(self.spark)
        image = 'mcr.microsoft.com/azure-sql-edge'
        dialect = 'mssql+pymssql'
        sql_server_container = SqlServerContainer(image, dialect=dialect)
        postgres_container = PostgresContainer("postgres:9.5")
        user = sql_server_container.SQLSERVER_USER
        password = sql_server_container.SQLSERVER_PASSWORD
        ms_sql_db = sql_server_container.SQLSERVER_DBNAME
        input_table_name = "test_input"
        input_schema = "test"
        output_path = os.path.join(os.curdir, 'output')
        query_file = os.path.join(os.curdir, '..', 'query.sql')
        with open(query_file, 'r') as file:
            query = file.read().replace("[dbo].[table_name]", "test.test_input")

        conf = {"input": {
            "query_file_path": query_file,
            "sql": {
                "host_name": "localhost",
                "port": 1433,
                "db_name": ms_sql_db,
                "user_name": user,
                "password": password,
                "table_name": "test_input"
            }
        },
            "output": {
                "datalake": {
                    "output_file_path": output_path,
                    "output_file_format": "parquet"
                },
                "sql": {
                    "host_name": "localhost",
                    "port": 5432,
                    "db_name": "test",
                    "user_name": "test",
                    "password": "test",
                    "table_name": "test_output"
                }
            }
        }

        with sql_server_container.with_bind_ports(1433, 1433) as mssql:
            print(sql_server_container.get_connection_url())
            connection_details = sql_server_container.get_connection_url()
            port = connection_details.split(":")[3].split("/")[0]
            conf['input']['sql']['port'] = int(port)
            conn = pymssql.connect(server="localhost",
                                   user=user, password=password, database=ms_sql_db,
                                   port=int(port), autocommit=True)
            print(sql_server_container.get_connection_url())
            cur = conn.cursor()
            cur.execute(f'CREATE SCHEMA {input_schema} ;')
            cur.execute(f"""CREATE TABLE  [{input_schema}].[{input_table_name}] 
                                         (  cowUNID VARCHAR(max), 
                                            sex VARCHAR(1),
                                            cowUNID_Father VARCHAR(max), 
                                            cowUNID_Mother VARCHAR(max) ) ;
                                """)
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_M1', 'M', 'cow_M0','cow_F0');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_F1', 'F', 'cow_M0','cow_F0');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_M2', 'M', 'cow_M-1','cow_F-1');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_F2', 'F', 'cow_M-1','cow_F-1');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_M3', 'M', 'cow_M1','cow_F2');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_F3', 'F', 'cow_M2','cow_F1');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_M4', 'M', 'cow_M2','cow_F3');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_F4', 'F', 'cow_M3','cow_F2');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_M5', 'M', 'cow_M3','cow_F4');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_F5', 'F', 'cow_M4','cow_F3');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_M6', 'M', 'cow_M4','cow_F5');")
            cur.execute(f"INSERT INTO {input_schema}.{input_table_name}  values('cow_F6', 'F', 'cow_M5','cow_F4');")
            conn.close()
            with postgres_container.with_bind_ports(5432, 5432):
                con = psycopg2.connect(
                    host="localhost",
                    database=postgres_container.POSTGRES_DB,
                    user=postgres_container.POSTGRES_USER,
                    port=5432,
                    password=postgres_container.POSTGRES_PASSWORD)
                con.autocommit = True
                cursor = conn.cursor()
                cursor.execute('CREATE SCHEMA test ;')
                cursor.execute("""CREATE TABLE  test.test_output(cowUNID VARCHAR, 
                                                              levelDepth integer, 
                                                               parent VARCHAR,
                                                               CHILD VARCHAR,
                                                               GRAND_CHILD VARCHAR,
                                                               GREAT_GRAND_CHILD VARCHAR
                                                              ) ;""")
                process_etl(spark_logger=spark_logger, conf=conf, spark=self.spark, query=query)

                cursor.execute("select count(*) count from test.test_output")
                record = cursor.fetchone()
                self.assertGreater(record['count'], 0)


if __name__ == '__main__':
    unittest.main()
