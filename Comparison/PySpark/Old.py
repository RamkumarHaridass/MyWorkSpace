import os
import sys
from datetime import datetime
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, Row
from pyspark.sql.functions import sha2, concat_ws, array, row_number, col, lit, when, count, sum
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import numpy as np
import collections
import io
import boto3
from airflow.operators.email import EmailOperator
from airflow.models import Variable

class RegressionJob:

    def __init__(self, app_name, baseline_table_name, regression_table_name, baseline_batch_ids, regression_batch_ids, primary_keys):
        self.app_name = app_name
        self.insert_timestamp = datetime.now()
        self.baseline_table_name = baseline_table_name
        self.baseline_batch_ids = baseline_batch_ids
        self.regression_batch_ids = regression_batch_ids
        self.regression_table_name = regression_table_name
        self.primary_keys = primary_keys
        self.sc = SparkContext("local", app_name)
        self.sc.setLogLevel('WARN')
        self.spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()

        self.s3_access_key = os.environ.get("access_key")
        self.s3_secret_key = os.environ.get("secret_key")
        self.s3_endpoint_url = os.environ.get("endpoint_url_https")
        self.s3_bucket_name = os.environ.get("bucket_name")
        self.hadoopConf = self.sc._jsc.hadoopConfiguration()
        self.hadoopConf.set("fs.s3a.access.key", os.environ.get('access_key'))
        self.hadoopConf.set("fs.s3a.secret.key", os.environ.get('secret_key'))
        self.hadoopConf.set("fs.s3a.endpoint", os.environ.get('endpoint_url_http'))

    def get_split_values(self, values):
        new = [x.strip() for x in values.split(',')]
        return new

    def duration_time(self, start_time, end_time):
        duration = end_time - start_time
        total_seconds = int(duration.total_seconds())
        duration_minutes = int(divmod(total_seconds, 60)[0])
        duration_seconds = int(divmod(total_seconds, 60)[1])

        if duration_minutes == 0:
            total_duration = str(duration_seconds) + " SECONDS"
            print("Time taken for the execution: " + total_duration)
        elif duration_seconds == 0:
            total_duration = str(duration_minutes) + " MINUTES"
            print("Time taken for the execution: " + total_duration)
        else:
            total_duration = str(duration_minutes) + " MINUTES & " + str(duration_seconds) + " SECONDS"
            print("Time taken for the execution: " + total_duration)
        return total_duration

    def compare(self):
        start_time = datetime.now()
        print("Execution started at :" + str(start_time))
        baseline_batch_ids = self.get_split_values(self.baseline_batch_ids)
        regression_batch_ids = self.get_split_values(self.regression_batch_ids)
        try:
            baseline_sql = f'SELECT * FROM {self.baseline_table_name} ' \
                           f'WHERE pcm_batch_id in ({str(baseline_batch_ids)[1:-1]} )'
            print(baseline_sql)
            regression_sql = f'SELECT * FROM {self.regression_table_name} ' \
                             f'WHERE pcm_batch_id in ({str(regression_batch_ids)[1:-1]} )'
            print(regression_sql)

            baseline_df = self.spark.sql(baseline_sql)
            print("Dataframe created Successfully for Baseline..")
            regression_df = self.spark.sql(regression_sql)
            print("Dataframe created Successfully for Regression..")
            baseline_df = baseline_df.select([col(c).cast("string") for c in baseline_df.columns])
            regression_df = regression_df.select([col(c).cast("string") for c in regression_df.columns])
            base_record_count = baseline_df.count()
            reg_record_count = regression_df.count()
            s3_client = boto3.client(
                service_name='s3',
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                endpoint_url=self.s3_endpoint_url,
            )
        except:
            print("Error while creating dataframe..Please check the query.Might be due to memory Isuue...")
            print(sys.exc_info()[0])

        if base_record_count == 0:
            print("Baseline Data has Zero records..")
        if reg_record_count == 0:
            print("Regression Data has Zero records..")
        if (base_record_count > 0 and base_record_count < 20000 and reg_record_count > 0 and reg_record_count < 20000):
            self.compare_pandas_df(baseline_df, regression_df, s3_client, base_record_count, reg_record_count)
        else:
            self.compare_pyspark_df(baseline_df, regression_df, s3_client, base_record_count, reg_record_count)

    def compare_pandas_df(self, baseline_df, regression_df, s3_client, base_record_count, reg_record_count):
        try:
            baseline_df = baseline_df.toPandas()
            regression_df = regression_df.toPandas()
        except:
            print("Error while converting pyspark to pandas datframe..")

        baseline_records = base_record_count
        regression_records = reg_record_count
        baseline_columns = baseline_df.shape[1]
        regression_columns = regression_df.shape[1]
        list1 = baseline_df.columns
        list2 = regression_df.columns
        matching_cols = set(list1) & set(list2)
        non_matching_cols = set(list1) ^ set(list2)
        matching_cols_count = len(matching_cols)
        non_matching_cols_count = len(non_matching_cols)
        if non_matching_cols_count != 0:
            print("Unmatched column names :", non_matching_cols)
        p_keys = self.primary_keys
        primary_keys = self.get_split_values(p_keys.strip())
        print("Primary keys :")
        print(primary_keys)
        table_name = self.baseline_table_name.split(".")
        product_name = "_".join(table_name[1].split('_')[1:-1]).upper()
        # source_system_id = list(set(baseline_df['position_source_system_id'].values))
        # source_system_region_name = list(set(baseline_df['position_source_system_region_name'].values))
        baseline_core_batch_id = list(set(baseline_df['core_batch_id'].values))
        regression_core_batch_id = list(set(regression_df['core_batch_id'].values))
        date1 = list(set(baseline_df['business_date'].values))
        n_date = pd.to_datetime(date1[0])
        business_date = n_date.strftime("%Y-%m-%d")
        baseline_df.reset_index(drop=True, inplace=True)
        regression_df.reset_index(drop=True, inplace=True)
        flag = "FAIL"
        if non_matching_cols_count == 0:
            flag = "PASS"
        if baseline_df.shape[0] > 0 and regression_df.shape[0] > 0:
            baseline_df = baseline_df[matching_cols]
            regression_df = regression_df[matching_cols]
            baseline_df.set_index(primary_keys, inplace=True)
            regression_df.set_index(primary_keys, inplace=True)
            baseline_df.sort_index(inplace=True)
            regression_df.sort_index(inplace=True)
            base_index_values = baseline_df.index.values
            reg_index_values = regression_df.index.values
            base_index_list = base_index_values.tolist()
            reg_index_list = reg_index_values.tolist()
            matching_positions = set(base_index_list) & set(reg_index_list)
            non_matching_positions = set(base_index_list) ^ set(reg_index_list)
            extra_baseline = baseline_df[baseline_df.index.isin(list(non_matching_positions))]
            extra_regression = regression_df[regression_df.index.isin(list(non_matching_positions))]
            baseline_df = baseline_df[baseline_df.index.isin(list(matching_positions))]
            regression_df = regression_df[regression_df.index.isin(list(matching_positions))]
            baseline_duplicate_key_count = baseline_df[baseline_df.index.duplicated()].shape[0]
            regression_duplicate_key_count = regression_df[regression_df.index.duplicated()].shape[0]
            baseline_duplicate_records = baseline_df[baseline_df.index.duplicated()]
            regression_duplicate_records = regression_df[regression_df.index.duplicated()]
            print("Removing Duplicates if any in primary keys..")
            baseline_df = baseline_df[~baseline_df.index.duplicated(keep='first')]

        if baseline_df.shape == regression_df.shape:
            try:
                result_df = baseline_df.compare(regression_df)
                result_df.rename(columns={'self': 'baseline data', 'other': 'regression data'}, level=1, inplace=True)
                result_df.drop('pcm_batch_id', axis=1, inplace=True)
                result_df.dropna(how='all', inplace=True)
                print("Comparison Completed..")
            except:
                print("Error while comparing data..")
                print(sys.exc_info()[0])
        else:
            print("Record count doesn't match.. Please check the primary keys..Might have occured due to duplicates in primary keys..")

        status = "FAIL"
        if result_df.empty == True:
            status = "PASS"
        print("Test Status : " + status)

        l1 = [
            "Test Status", "Product Name", "Business Date", "Baseline PCM Batch ID", "Regression PCM Batch ID",
            "Baseline CORE Batch ID", "Regression CORE Batch ID", "Columns present in Baseline",
            "Columns present in Regression", "Records present in Baseline", "Records present in Regression",
            "Primary Key Column Count", "Baseline Duplicate Records Count", "Regression Duplicate Records Count",
            "Mismatched Records Count", "Mismatched Columns Count", "All Columns Mapped", "Unmapped Columns Count"
        ]
        l2 = [
            status, product_name, str(business_date), self.baseline_batch_ids, self.regression_batch_ids,
            str(baseline_core_batch_id[0]), str(regression_core_batch_id[0]), baseline_columns, regression_columns,
            baseline_records, regression_records, len(primary_keys), baseline_duplicate_key_count,
            regression_duplicate_key_count, int(result_df.shape[0]), int(result_df.shape[1] / 2), flag,
            non_matching_cols_count
        ]
        summary = pd.DataFrame(list(zip(l1, l2)))
        if table_name[1] == "ipv_position_consumption":
            product_name = "IPV"
        tc_no = 'TC_' + str(product_name).replace(' ', '_') + '_REGRESSION'
        summary.rename(columns={0: 'Test Case Name', 1: tc_no}, inplace=True)
        summary.set_index(['Test Case Name', tc_no], inplace=True)
        # summary.head(15)
        mismatched_cols = list(result_df.columns.levels[0])
        mismatched_cols.remove('pcm_batch_id')
        mismatched_columns = pd.DataFrame(list(zip(mismatched_cols)))
        mismatched_columns.rename(columns={0: 'Mismatched Column Names'}, inplace=True)
        unmapped_columns = pd.DataFrame(list(zip(non_matching_cols)))
        unmapped_columns.rename(columns={0: 'UnMatched Column Names'}, inplace=True)
        mismatch_info = result_df.count().to_frame()
        mismatch_info.rename(columns={0: 'Record Count'}, inplace=True)
        timestamp = datetime.now().strftime("%Y_%m_%d_%M")
        file_name = 'qa_regression_reports/' + str(product_name) + '_REPORT_' + str(timestamp) + '.xlsx'
        print("Creating Report..")
        try:
            with io.BytesIO() as output:
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    summary.to_excel(writer, sheet_name="Summary Report", header=None)
                    if result_df.empty == False:
                        mismatched_columns.to_excel(writer, sheet_name="Mismatched Column Names", index=False)
                        mismatch_info.to_excel(writer, sheet_name="Mismatched Records Count")
                        result_df.to_excel(writer, sheet_name="Mismatched Data")
                    if extra_baseline.empty == False:
                        print("Extra Records in Baseline : " + str(extra_baseline.shape))
                        extra_baseline.to_excel(writer, sheet_name="Extra Records in Baseline")
                    if extra_regression.empty == False:
                        print("Extra Records in Regression : " + str(extra_regression.shape))
                        extra_regression.to_excel(writer, sheet_name="Extra Records in Regression")
                    if baseline_duplicate_key_count > 0:
                        baseline_duplicate_records.to_excel(writer, sheet_name="Baseline Duplicate Records")
                    if regression_duplicate_key_count > 0:
                        regression_duplicate_records.to_excel(writer, sheet_name="Regression Duplicate Records")
                    if baseline_columns != regression_columns:
                        unmapped_columns.to_excel(writer, sheet_name="UnMatched Column Names", index=False)
                data = output.getvalue()
            response = s3_client.put_object(Bucket=self.s3_bucket_name, Key=file_name, Body=data)
            s3_status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if s3_status == 200:
                print(f"Successful S3 put_object response. Status - {s3_status}")
                print("Report Generated Successfuly..")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {s3_status}")
        except:
            print("Error while writing report..")
            print(sys.exc_info()[0])
        self.email_notification(summary.to_html(header=False), tc_no, status)

    def compare_pyspark_df(self, baseline_df, regression_df, s3_client, base_record_count, reg_record_count):
        print("Pyspark Comapre")
        baseline_df = baseline_df.drop("pcm_batch_id")
        regression_df = regression_df.drop("pcm_batch_id")
        base_list = baseline_df.columns
        reg_list = regression_df.columns
        cols = set(base_list) & set(reg_list)
        non_matching_cols = set(base_list) ^ set(reg_list)
        table_name = self.baseline_table_name.split(".")
        product_name = "_".join(table_name[1].split('_')[1:-1]).upper()
        if table_name[1] == "ipv_position_consumption":
            product_name = "IPV"
        flag = "FAIL"
        if (len(non_matching_cols) == 0):
            flag = "PASS"
        PK = self.get_split_values(self.primary_keys.strip())
        b = baseline_df.first()['business_date']
        n_date = pd.to_datetime(b)
        business_date = n_date.strftime("%Y-%m-%d")
        base_core_batch_id = baseline_df.first()['core_batch_id']
        reg_core_batch_id = regression_df.first()['core_batch_id']
        base_pk_df = baseline_df.select(PK)
        reg_pk_df = regression_df.select(PK)
        base_pk_df.cache()
        reg_pk_df.cache()
        duplicate_base = base_pk_df.groupBy(PK).count().filter('count>1')
        duplicate_reg = reg_pk_df.groupBy(PK).count().filter('count>1')
        # duplicate_base_count = duplicate_base.count()
        # duplicate_reg_count = duplicate_reg.count()
        extra_base = base_pk_df.subtract(reg_pk_df)
        extra_reg = reg_pk_df.subtract(base_pk_df)
        # extra_base_count = extra_base.count()
        # extra_reg_count = extra_reg.count()
        baseline_df = baseline_df.dropDuplicates(PK).withColumn("DfHash", sha2(concat_ws("||", *PK), 256))
        regression_df = regression_df.dropDuplicates(PK).withColumn("DfHash", sha2(concat_ws("||", *PK), 256))
        p_hash = base_pk_df.dropDuplicates(PK).withColumn("DfHash", sha2(concat_ws("||", *PK), 256))
        r_hash = reg_pk_df.dropDuplicates(PK).withColumn("DfHash", sha2(concat_ws("||", *PK), 256))
        index = p_hash.join(r_hash, "DfHash", how='inner').select("DfHash")
        baseline_df = baseline_df.join(index, "DfHash", how='inner').drop("DfHash")
        regression_df = regression_df.join(index, "DfHash", how='inner').drop("DfHash")
        del index, p_hash, r_hash
        df1x = (baseline_df.withColumn("DfHash", sha2(concat_ws("||", *cols), 256)).select(*PK, "DfHash"))
        df2x = (regression_df.withColumn("DfHash", sha2(concat_ws("||", *cols), 256)).select(*PK, "DfHash"))
        hash_with_cnt_df1 = df1x.groupBy(*PK, "DfHash").agg(count("*").alias("record_count_df1"))
        hash_with_cnt_df2 = df2x.groupBy(*PK, "DfHash").agg(count("*").alias("record_count_df2"))
        del df1x, df2x
        mismatch_id_position = hash_with_cnt_df1.join(hash_with_cnt_df2, on=[*PK, "DfHash"], how='outer') \
            .where("""record_count_df1 is null or record_count_df2 is null or record_count_df1<>record_count_df2""")
        mismatch_ids = mismatch_id_position.select(*PK, "DfHash").dropDuplicates()
        mismatched_df1 = baseline_df.join(mismatch_ids, PK, how='inner')
        mismatched_df2 = regression_df.join(mismatch_ids, PK, how='inner')
        del baseline_df, regression_df
        mismatched_df1 = mismatched_df1.select([F.col(c).alias(c + "_b") for c in mismatched_df1.columns])
        mismatched_df2 = mismatched_df2.select([F.col(c).alias(c + "_r") for c in mismatched_df2.columns])
        out = mismatched_df1.join(mismatched_df2, mismatched_df1.DfHash_b == mismatched_df2.DfHash_r, how="outer")
        final_df = out.select([
            when(~mismatched_df1[c + '_b'].eqNullSafe(mismatched_df2[c + '_r']),
                 array(mismatched_df1[c + '_b'].cast("string"),
                       mismatched_df2[c + '_r'].cast("string"),
                       mismatched_df1['DfHash_b'].cast("string"))).alias(c)
            for c in cols
        ]) \
            .selectExpr('stack({},{}) as ( colName, mismatch)'.format(
                len(cols), ','.join('"{0}",`{0}`'.format(c) for c in cols))) \
            .filter('mismatch is not NULL')
        result = final_df.select(
            final_df.mismatch[2].alias("key"),
            "colName",
            final_df.mismatch[0].alias("baseline_data"),
            final_df.mismatch[1].alias("regression_data")
        )
        del final_df, out
        final_res = result.join(mismatch_ids, result.key == mismatch_ids.DfHash, how="inner").drop('key', 'DfHash')
        final_col = PK
        final_col.append("colName")
        final_col.append("baseline_data")
        final_col.append("regression_data")
        final_res = final_res.select(final_col)
        timestamp = datetime.now().strftime("%Y_%m_%d_%M")
        output_location = 's3a://' + str(self.s3_bucket_name) + '/qa_regression_reports/' + str(product_name) + '_REPORT_' + str(timestamp)
        loc = 'qa_regression_reports/' + str(product_name) + '_REPORT_' + str(timestamp)
        final_res.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("compression", "none").option("header", "true").save(output_location)
        del final_res
        res_df = self.spark.read.option("header", True).csv(output_location)
        res_df.show(10)
        mismatch_id_count = res_df.select(PK).distinct().count()
        status = 'FAIL'
        if mismatch_id_count == 0:
            status = 'PASS'
        extra_base_count = 0
        extra_reg_count = 0
        duplicate_base_count = 0
        duplicate_reg_count = 0
        if extra_base.head(1):
            extra_base.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("compression", "none").option("header", "true").save(output_location + '/extra_baseline_records')
            extra_base = self.spark.read.option("header", True).csv(output_location + '/extra_baseline_records')
            extra_base_count = extra_base.count()
        if extra_reg.head(1):
            extra_reg.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("compression", "none").option("header", "true").save(output_location + '/extra_regression_records')
            extra_reg = self.spark.read.option("header", True).csv(output_location + '/extra_regression_records')
            extra_reg_count = extra_reg.count()
        if duplicate_base.head(1):
            duplicate_base.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("compression", "none").option("header", "true").save(output_location + '/duplicate_baseline_records')
            duplicate_base = self.spark.read.option("header", True).csv(output_location + '/duplicate_baseline_records')
            duplicate_base_count = duplicate_base.count()
        if duplicate_reg.head(1):
            duplicate_reg.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("compression", "none").option("header", "true").save(output_location + '/duplicate_regression_records')
            duplicate_reg = self.spark.read.option("header", True).csv(output_location + '/duplicate_regression_records')
            duplicate_reg_count = duplicate_reg.count()
        mismatch_column = res_df.select("colName").distinct()
        mismatch_column_count = mismatch_column.count()
        print("Generating Sample test report..")
        unmapped_columns = pd.DataFrame(list(zip(non_matching_cols)))
        unmapped_columns.rename(columns={0: 'UnMatched Column Names'}, inplace=True)
        l1 = [
            "Test Status", "Product Name", "Business Date", "Baseline PCM Batch ID", "Regression PCM Batch ID",
            "Baseline CORE Batch ID", "Regression CORE Batch ID", "Columns present in Baseline",
            "Columns present in Regression", "Records present in Baseline", "Records present in Regression",
            "Primary Key Column Count", "Extra Records present in Baseline", "Extra Records present in Regression",
            "Baseline Duplicate Records Count", "Regression Duplicate Records Count", "Mismatched Records Count",
            "Mismatched Columns Count", "All Columns Mapped", "Unmapped Columns Count"
        ]
        l2 = [
            status, product_name, str(business_date), self.baseline_batch_ids, self.regression_batch_ids,
            int(base_core_batch_id), int(reg_core_batch_id), int(len(base_list)), int(len(reg_list)),
            base_record_count, reg_record_count, len(PK), extra_base_count, extra_reg_count,
            duplicate_base_count, duplicate_reg_count, mismatch_id_count, mismatch_column_count,
            flag, int(len(non_matching_cols))
        ]