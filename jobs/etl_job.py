import argparse

from pyspark.sql import SparkSession

from jobs.ConfParser import ConfParser
import logging


class EtlJob:
    def __init__(self, conf: dict, spark: SparkSession, logger: logging):
        self.config = conf
        self.spark = spark
        self.log = logger

    def write_date(self, df):
        """
        write df as write
        :param df:
        :return:
        """
        self.log.info("writing data to data lake")
        if self.config['output']['datalake']['output_file_format'] == 'csv':
            df.write.mode("overwrite") \
                .option("header", True) \
                .csv(self.config['output']['datalake']['output_file_path'])

        elif self.config['output']['datalake']['output_file_format'] == 'parquet':
            df.write.mode("overwrite") \
                .parquet(self.config['output']['datalake']['output_file_path'])

        elif self.config['output']['datalake']['output_file_format'] == 'orc':
            df.write.mode("overwrite") \
                .orc(self.config['output']['datalake']['output_file_path'])

        else:
            self.log.error("Invalid file format, {} this file format is supported "
                           .format(self.config['output']['datalake']['output_file_path']))

    def write_to_rdbms(self, df):
        """
        writes data to rdbms
        :param df: data frame
        :return: None
        """
        df.write.format('jdbc').options(url='jdbc:postgresql://%s:%s/%s' % (
            self.config['output']['sql']['host_name'], self.config['output']['sql']['port'],
            self.config['output']['sql']['db_name']),
                                        driver='org.postgresql.Driver',
                                        dbtable=self.config['output']['sql']['table_name'],
                                        user=self.config['output']['sql']['user_name'],
                                        password=self.config['output']['sql']['password']).mode('append').save()

    def read_from_sql(self, query):
        """
        read the data from sql
        :return:
        """
        host = self.config['input']['sql']['host_name']
        port = self.config['input']['sql']['port']
        db = self.config['input']['sql']['db_name']
        return self.spark.read.format("jdbc")\
            .option("url", f"jdbc:sqlserver://{host}:{port};databaseName={db};encrypt=true;trustServerCertificate=true;") \
            .option("query", f'{query}') \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .option("user", self.config['input']['sql']['user_name']) \
            .option("password", self.config['input']['sql']['password']).load()


def process_etl(conf, spark_logger, spark, query):
    """
     process etl
    :param conf: dict
    :param spark_logger: logging
    :param spark: sparkSession
    :param query: str
    :return: None
    """
    spark_logger.info("got valid conf")
    etl = EtlJob(spark=spark, conf=conf, logger=spark_logger)
    spark_logger.info("reading data from rdbms")
    raw_df = etl.read_from_sql(query)
    spark_logger.info("writing data to datalake")
    etl.write_date(df=raw_df)
    spark_logger.info("writing to rdbms")
    etl.write_to_rdbms(df=raw_df)


def main(conf_file):
    """Main ETL script definition.
    :return: None
    """
    conf = ConfParser(conf_file=conf_file)
    config = conf.parse_config()
    spark = SparkSession.builder.master("local[*]").appName("ETL").getOrCreate()
    spark_logger = logging.Log4j(spark)
    query = conf.get_input_query()
    process_etl(conf=config, spark=spark, spark_logger=spark_logger, query=query)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--conf", help="conf file", required=True)
    args = parser.parse_args()
    main(args.conf)
