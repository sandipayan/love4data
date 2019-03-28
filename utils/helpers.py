import os
import sys
import re
import boto3
from datetime import datetime

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.storagelevel import StorageLevel

MEMORY_AND_DISK = StorageLevel(True, True, False, False)

def splitter(array, n=50):
    assert n > 0
    i = 0
    result = []
    for e in array:
        if i < n:
            result.append(e)
            i += 1
        if i >= n:
            yield result
            i = 0
            del result[:]

    if len(result):
        yield result


def gen_partition_statement(partition_tuples, target_root, run_id=None):
    """
    partition_tuples = [
        ('brand', 'eat24'),
        ('dt', '2007-10-14'),
    ]
    """
    if run_id is not None:
        partition_tuples = [('run_id', run_id)] + partition_tuples
    # todo: part_a1, part_a2, part_b, part_c, part_what? you lost me.
    part_a1 = ", ".join(
        ["{label}='{value}'".format(label=i[0], value=i[1]) for i in partition_tuples]
    )
    part_a2 = "/".join(
        ["{label}={value}".format(label=i[0], value=i[1]) for i in partition_tuples]
    )
    part_b = "partition ({partitions_str})".format(partitions_str=part_a1)
    part_c = "location '{location}'".format(location=os.path.join(target_root, part_a2))
    return part_b + ' ' + part_c


def merge_partition_names_values(partition_names, hive_partitions):
    output = []
    for p in hive_partitions:
        temp_val = list(p)
        if isinstance(temp_val, basestring):
            temp_val = [temp_val]
        output.append([(partition_names[i], temp_val[i]) for i in range(len(partition_names))])
    return output


def cast_to_hive_type(sc, df, table_name):
    hivedf = sc.sql("select * from {target_table} limit 1".format(target_table = table_name))
    convdict = {'int': IntegerType(), 'string': StringType(), 'timestamp': TimestampType(),
                'boolean' : BooleanType(), 'double': DoubleType(), 'bigint': LongType(),
                'date': DateType(), 'decimal': DecimalType(), 'float': FloatType()}

    # This assumes that column names in both hive and dataframe is same, which is not necessary
    # so, the comparison could have been done based on positions, instead of names ideally
    if not sorted([(i[0].lower(), i[1].lower()) for i in df.dtypes]) \
           == sorted([(i[0].lower(), i[1].lower()) for i in hivedf.dtypes]):
        diffs = list(set([i[0] for i in set(hivedf.dtypes) ^ set(df.dtypes)]))
        for _i in range(len(diffs)):
            toType = convdict[dict(hivedf.dtypes)[diffs[_i]]]
            df = df.withColumn(diffs[_i], F.col(diffs[_i]).cast(toType))
    return df


def create_schema(sc, table_schema):
    schema, _ = table_schema.split(".")
    sc.sql("CREATE DATABASE IF NOT EXISTS {db}".format(db=schema))


def create_hist_table(sc, table_name, history_table_name):
    """
    If history_table_name is Not None and its not created yet by a provided template, create it here
    """
    tgt_db, tgt_tbl = table_name.split(".")
    hist_db, hist_tbl = history_table_name.split(".")

    hist_db_tables = [_i.name for _i in sc.catalog.listTables(hist_db)]

    if hist_tbl not in hist_db_tables:
        ddl = sc.sql("show create table {}".format(table_name)).collect()[0][0]
        new_ddl = ddl.replace('TABLE', ' TABLE IF NOT EXISTS ').replace(tgt_db, hist_db, 1).replace(tgt_tbl,hist_tbl) \
            .replace(')',' ) PARTITIONED BY (run_id string) ', 1)
        sc.sql(new_ddl)


def purge_history(sc, table, history_table, keep_latest_n):
    """ This will create and purge history table. If no history table is passed
    then still the old S3 data needs to be removed, because every time current table points to a new location """

    if sys.platform != "darwin":
        # remove the corresponding s3 location - safety check that the location is a run_id location in particular buckets.
        # wants to make sure we are deleting s3 path with expected pattern.
        # Expected S3 path is in the format of  - {some s3 bucket}/{some folder}/{**optional subfolders**}/{job name folder}/{folder with run id}/*.{file extension}

        path_regex=re.compile('s3://MyCompany[.a-z_-]*/[.a-z_-]*(/[.a-z_-]*)?/[a-z-_]*/run_id=\d{8}_\d{4}')
        path_regex_group = re.compile(r'^s3://(?P<bucket>.*?)/(?P<key>.*)')

        client = boto3.client('s3')
        s3 = boto3.resource('s3')
        s3_rm_path = []
        keys_in_parent = []
        keys_to_purge = []

        if history_table is not None:
            partitions = sc.sql("show partitions {hist_table_name}".format(hist_table_name=history_table)).collect()

            #  modifying this code as higher version of hive has key as 'partition', instead of 'result' . Hive 2.3.3-amzn-2
            # partitions = [_i.result for _i in partitions]

            partitions = [_i.asDict().values()[0] for _i in partitions]
            partitions.sort(reverse=True)

            if len(partitions) > keep_latest_n:
                partitions_to_purge = partitions[keep_latest_n:]

                for _i in range(len(partitions_to_purge)):
                    partition_val = partitions_to_purge[_i].split('=')[1]
                    df = sc.sql("describe formatted {hist_table_name} partition (run_id='{partition_val}')".format(hist_table_name=history_table, partition_val=partition_val))

                    s3_rm_path.append(df.where(df.col_name.startswith('Location')).select('data_type').collect()[0]['data_type'])

                    # drop this partition from the table
                    sc.sql("alter table {hist_table_name} drop if exists partition (run_id='{partition_val}')".format(hist_table_name=history_table, partition_val=partition_val))

        else:
            # delete old s3 run_ids which will be there in the parent folder
            df = sc.sql("describe formatted {table_name}".format(table_name=table))
            location = df.where(df.col_name.startswith('Location')).select('data_type').collect()[0]['data_type']
            m = re.match(path_regex_group, location).groupdict()
            bucket_name = m['bucket']
            parent_key = m['key'].split("=")[0].replace("run_id", "")
            response = client.list_objects_v2(Bucket=bucket_name, Prefix=parent_key, Delimiter="/")
            list_of_keys = [i['Prefix'] for i in response['CommonPrefixes']]

            for i in list_of_keys:
                keys_in_parent.append(i.split("run_id=")[1].replace("/", ""))

            keys_in_parent.sort(reverse=True)

            if len(keys_in_parent) > keep_latest_n:
                keys_to_purge = keys_in_parent[keep_latest_n:]

            for _i in keys_to_purge:
                s3_rm_path.append(os.path.join("s3://", bucket_name, parent_key, "run_id="+_i))

        # remove the paths from s3
        for _i in s3_rm_path:
            if re.match(path_regex, _i):
                m = re.match(path_regex_group, _i).groupdict()
                bucket = s3.Bucket(m['bucket'])
                for obj in bucket.objects.filter(Prefix=m['key']):
                    s3.Object(bucket.name, obj.key).delete()


def write_and_partition(sc, df, target_location, table_name, history_table_name=None, number_of_files=10, run_id=None,
                        table_create_statements=None, partition_names=None, keep_latest_n=7):

    # Defaulting to parallel write to S3, with 10 files - individual job can change this
    sc.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    create_schema(sc, table_name)

    if history_table_name is not None:
        create_schema(sc, history_table_name)

    for c in table_create_statements:
        sc.sql(c)

    if history_table_name is not None:
        create_hist_table(sc,table_name, history_table_name)

    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M")
    run_str = "run_id=" + run_id

    run_directory = os.path.join(target_location, run_str)

    df = cast_to_hive_type(sc, df, table_name)

    df.repartition(number_of_files).write.parquet(path=run_directory, mode="append", partitionBy=partition_names)

    # main table
    alter_statement_change_location_main = """
        alter table {table_name}
        set location '{location}'
        """.format(table_name=table_name, location=run_directory)
    sc.sql(alter_statement_change_location_main)

    # history table
    if history_table_name is not None and partition_names is None:
        alter_statement_add_partition_history = """
            alter table {table_name}
            add if not exists partition (run_id='{run_id}') location '{location}'
            """.format(table_name=history_table_name, run_id=run_id, location=run_directory)
        sc.sql(alter_statement_add_partition_history)

    if partition_names is not None:
        if not df.rdd.isEmpty():

            df = df.persist(MEMORY_AND_DISK)
            hive_partitions = list(df.select(*partition_names).dropDuplicates().collect())
            hive_partitions_with_names = merge_partition_names_values(partition_names, hive_partitions)

            # for main table
            for batch in splitter([gen_partition_statement(ps, run_directory)
                                   for ps in hive_partitions_with_names]):
                partition_statement = "alter table {table_name} add if not exists " \
                                          .format(table_name=table_name) + ' '.join(batch)
                sc.sql(partition_statement)

            # and for history table
            if history_table_name is not None:
                for batch in splitter([gen_partition_statement(ps, target_location, run_id)
                                       for ps in hive_partitions_with_names]):
                    partition_statement = "alter table {table_name} add if not exists " \
                                              .format(table_name=history_table_name) + ' '.join(batch)
                    sc.sql(partition_statement)

    # for all cases call purge_history
    purge_history(sc, table_name, history_table_name, keep_latest_n)