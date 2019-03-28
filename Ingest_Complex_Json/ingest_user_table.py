import pytz
import datetime
import argparse

import logging
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, input_file_name

from utils.helpers import *
from Ingest_Complex_Json.ddls import *


CURRENT_DATE = datetime.datetime.now().strftime('%Y-%m-%d')


class Processor:
    def __init__(self, sc, args):
        self.sc = sc
        self.user_profile_input_location = args.input_location
        self.user_profile_stg_location = args.output_stg_location


    def process(self):

        def find_brand(s3_path):
            if s3_path.count('xxx') > 0:
                return 'type1'
            elif s3_path.count('yyy') > 0:
                return 'type2'
            elif s3_path.count('zzz') > 0:
                return 'type3'
            else:
                'invalid type'

        udf_find_brand = udf(find_brand, StringType())

        df_stg_user_profile = self.sc.read.json( self.user_profile_input_location, schema=user_tbl_json_ingest_schema) \
            .withColumn("s3_path", input_file_name()) \
            .withColumn("brand", udf_find_brand('s3_path')).drop('s3_path')

        df_stg_user_profile.printSchema()

        write_and_partition(self.sc, df_stg_user_profile, self.user_profile_stg_location,
                            stg_user_table, number_of_files=200,keep_latest_n=2,
                            table_create_statements=[
                                create_stg_user_tbl_template.format(location=self.user_profile_stg_location)])



def user_profile_data_parse_args(args):
    description = "spark etl Job for user_profile data"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-n", "--app_name", help="user_profile", required=True)
    parser.add_argument("-i", "--input_location", nargs='+', default=[], help=" user_profile raw data S3 location", required=True)
    parser.add_argument("-os", "--output_stg_location", help="hive location foruser_profile staging data", required=True)
    return parser.parse_args(args)


def main(args):
    job_name = os.path.splitext(os.path.basename(os.path.abspath(__file__)))[0]

    cl_args = user_profile_data_parse_args(args)

    job_name = '{0}-{1}' \
        .format(job_name, '_'.join(str(datetime.datetime.now(pytz.timezone('America/Chicago'))).split(' ')))

    sc = SparkSession \
        .builder \
        .appName(job_name) \
        .enableHiveSupport() \
        .getOrCreate()

    processor = Processor(sc, cl_args)
    processor.process()
    sc.stop()
