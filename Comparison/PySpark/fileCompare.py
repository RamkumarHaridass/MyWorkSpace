# To script will be called from aperture_cluster.sh shell script, where all spark details are configured

import os
import sys
import timeit
import logging
import datetime
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, Row
from pyspark.sql.functions import sha2, concat_ws, array, row_number, col, lit, when, count, sum, desc, rank
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import numpy as np
import collections
import io

hdfsOutLocation = "[INPUT_HDFS_LOCATION]"
baseQuery = "[INPUT_BASE_QUERY]"
actualQuery = "[INPUT_ACTUAL_QUERY]"
keys = "[INPUT_KEYS]"
columnsToIgnore = "[INPUT_EXCLUDE_COLUMNS]"
fullDiffRequired = "[INPUT_FULL_REPORT]"

def splitValuesByComma(values):
    # Split the given values by commas
    new = [x.strip() for x in values.split(',')]
    return new

def highLevelDetails(baseQuery, actualQuery, ignoreColumns):
    # Initilize spark session
    spark = SparkSession.builder.appName("Concatenate").getOrCreate()
    # Run given query and store as Dataframe
    baseDf = spark.sql(baseQuery)
    logging.info("Dataframe created for Baseline")
    actualDf = spark.sql(actualQuery)
    logging.info("Dataframe created for Actual")
    baseColumnNames = baseDf.columns
    actualColumnNames = actualDf.columns
    # Get all matched columns
    matchedColumns = set(baseColumnNames) & set(actualColumnNames)
    logging.info("Total Columns matched in base and actual before Ignore = {}".format(len(matchedColumns)))
    # Drop ignored columns
    for columnToDrop in ignoreColumns:
        baseDf = baseDf.drop(columnToDrop)
        actualDf = actualDf.drop(columnToDrop)
    baseDf = baseDf.select([col(c).cast("string") for c in baseDf.columns])
    actualDf = actualDf.select([col(c).cast("string") for c in actualDf.columns])
    baseRecordCount = baseDf.count()
    logging.info("Total Records in Base = {}".format(baseRecordCount))
    actualRecordCount = actualDf.count()
    logging.info("Total Records in Actual = {}".format(actualRecordCount))
    if baseRecordCount == 0 or actualRecordCount == 0:
        logging.info("Actual or Base contains Zero records")
        exit()
    elif baseRecordCount == 0 and actualRecordCount == 0:
        logging.info("Both Actual and Base contains Zero records")
        exit()
    else:
        logging.info("Both Actual and Base contains Data, Hence proceed further for compare")
    baseColumnNames = baseDf.columns
    actualColumnNames = actualDf.columns
    matchedColumns = set(baseColumnNames) & set(actualColumnNames)
    logging.info("Total Columns matched in base and actual after Ignore = {}".format(len(matchedColumns)))
    # TODO : Need to add this unmatched columns in report
    nonMatchedColumns = set(baseColumnNames) ^ set(actualColumnNames)
    if len(nonMatchedColumns) != 0:
        logging.info("Total Columns not matched in base and actual = {}, Columns:-{}".format(len(nonMatchedColumns), nonMatchedColumns))
    else:
        logging.info("Total Columns not matched in base and actual = {}".format(len(nonMatchedColumns)))
    return baseDf, actualDf, matchedColumns

def duplicateChecker(baseDf, actualDf, primaryKey):
    # Check if the given keys contains duplicates
    # These duplicates will be added in reports
    global basePKDf
    basePKDf = baseDf.select(primaryKey)
    global actualPKDf
    actualPKDf = actualDf.select(primaryKey)
    basePKDf.cache()
    actualPKDf.cache()
    baseDuplicates = basePKDf.groupBy(primaryKey).count().filter('count>1').orderBy(desc("count"))
    bDCount = baseDuplicates.count()
    logging.info('Keys duplicated in base = {}'.format(bDCount))
    if baseDuplicates.head(1):
        if bDCount < 500:
            baseDuplicates.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/BaseDuplicates')
        else:
            baseDuplicatesFilt = baseDuplicates.withColumn("row", row_number().over(
                Window.orderBy(desc("count")))).filter(col("row") < 500).drop("row")
            baseDuplicatesFilt.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/BaseDuplicates')
    actualDuplicates = actualPKDf.groupBy(primaryKey).count().filter('count>1').orderBy(desc("count"))
    aDCount = actualDuplicates.count()
    logging.info('Keys duplicated in actual = {}'.format(aDCount))
    if actualDuplicates.head(1):
        if aDCount < 500:
            actualDuplicates.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/ActualDuplicates')
        else:
            actDuplicatesFilt = actualDuplicates.withColumn("row", row_number().over(
                Window.orderBy(desc("count")))).filter(col("row") < 500).drop("row")
            actDuplicatesFilt.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/ActualDuplicates')

def extraDataChecker(hdfsOutLocation, primaryKey):
    # This function will check if given key combination in base exists in actual and vice versa
    # Extra records will be added in reports
    extraInBase = basePKDf.subtract(actualPKDf)
    baseExtraCount = extraInBase.count()
    logging.info("Total Extra records in Base = {}".format(baseExtraCount))
    if extraInBase.head(1):
        if baseExtraCount < 500:
            extraInBase.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/BaseExtra')
        else:
            extraInBaseFilt = extraInBase.withColumn("row", row_number().over(
                Window.orderBy(primaryKey))).filter(col("row") < 500).drop("row")
            extraInBaseFilt.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/BaseExtra')
    extraInActual = actualPKDf.subtract(basePKDf)
    actualExtraCount = extraInActual.count()
    logging.info("Total Extra records in Actual = {}".format(actualExtraCount))
    if extraInActual.head(1):
        if actualExtraCount < 500:
            extraInActual.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/ActualExtra')
        else:
            extraInActualFilt = extraInActual.withColumn("row", row_number().over(
                Window.orderBy(primaryKey))).filter(col("row") < 500).drop("row")
            extraInActualFilt.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/ActualExtra')
    return baseExtraCount, actualExtraCount

def dropDuplicates(baseDf, actualDf, primaryKey):
    # Key duplicates will be dropped so that it won't impact compare performance
    baseDf = baseDf.dropDuplicates(primaryKey).withColumn("DfHash", sha2(concat_ws("||", *primaryKey), 256))
    actualDf = actualDf.dropDuplicates(primaryKey).withColumn("DfHash", sha2(concat_ws("||", *primaryKey), 256))
    baseHash = basePKDf.dropDuplicates(primaryKey).withColumn("DfHash", sha2(concat_ws("||", *primaryKey), 256))
    actualHash = actualPKDf.dropDuplicates(primaryKey).withColumn("DfHash", sha2(concat_ws("||", *primaryKey), 256))
    index = baseHash.join(actualHash, "DfHash", how='inner').select("DfHash")
    baseDf = baseDf.join(index, "DfHash", how='inner').drop("DfHash")
    actualDf = actualDf.join(index, "DfHash", how='inner').drop("DfHash")
    del index, baseHash, actualHash
    return baseDf, actualDf

def pysparkCompare(baseDf, actualDf, matchedColumns, primaryKey):
    # Identify mismatch columns and records
    baseDfTemp = (baseDf.withColumn("DfHash", sha2(concat_ws("||", *matchedColumns), 256)).select(*primaryKey, "DfHash"))
    actualDfTemp = (actualDf.withColumn("DfHash", sha2(concat_ws("||", *matchedColumns), 256)).select(*primaryKey, "DfHash"))
    baseHashWithCount = baseDfTemp.groupBy(*primaryKey, "DfHash").agg(count("*").alias("record_count_df1"))
    actualHashWithCount = actualDfTemp.groupBy(*primaryKey, "DfHash").agg(count("*").alias("record_count_df2"))
    del baseDfTemp, actualDfTemp
    mismatch_id_position = baseHashWithCount.join(actualHashWithCount, on=[*primaryKey, "DfHash"], how='outer') \
        .where("""record_count_df1 is null or record_count_df2 is null or record_count_df1<>record_count_df2""")
    mismatch_ids = mismatch_id_position.select(*primaryKey, "DfHash").dropDuplicates()
    mismatched_df1 = baseDf.join(mismatch_ids, primaryKey, how='inner')
    mismatched_df2 = actualDf.join(mismatch_ids, primaryKey, how='inner')
    del baseDf, actualDf
    mismatched_df1 = mismatched_df1.select([F.col(c).alias(c + "_b") for c in mismatched_df1.columns])
    mismatched_df2 = mismatched_df2.select([F.col(c).alias(c + "_r") for c in mismatched_df2.columns])
    out = mismatched_df1.join(mismatched_df2, mismatched_df1.DfHash_b == mismatched_df2.DfHash_r, how="outer")
    final_df = out.select([when(~mismatched_df1[c + '_b'].eqNullSafe(mismatched_df2[c + '_r']),
                               array(mismatched_df1[c + '_b'].cast("string"),
                                     mismatched_df2[c + '_r'].cast("string"),
                                     mismatched_df1['DfHash_b'].cast("string"))).alias(c)
                      for c in matchedColumns]) \
        .selectExpr('stack({},{}) as ( colName, mismatch)'.format(
            len(matchedColumns), ','.join('"{0}",`{0}`'.format(c) for c in matchedColumns))) \
        .filter('mismatch is not NULL')
    result = final_df.select(final_df.mismatch[2].alias("key"), "colName",
                            final_df.mismatch[0].alias("baseline_data"),
                            final_df.mismatch[1].alias("actual_data"))
    del final_df, out
    final_res = result.join(mismatch_ids, result.key == mismatch_ids.DfHash, how="inner").drop('key', 'DfHash')
    return final_res

def writeMismatchToFile(primaryKey, finalMissMatch, hdfsOutLocation, colDiffReq, fullDiffRequired):
    final_col = primaryKey
    final_col.append("colName")
    final_col.append("baseline_data")
    final_col.append("actual_data")
    finalMissMatch = finalMissMatch.select(final_col)
    if finalMissMatch.head(1):
        finalMissMatch = finalMissMatch.dropDuplicates()
        logging.info("Identifying Columns Differed")
        if fullDiffRequired == "true":
            finalMissMatch.coalesce(1).write.mode("overwrite").option("header", "true").format(
                "com.databricks.spark.csv").save(hdfsOutLocation + '/Diffs')
        else:
            logging.info('Skipping full Diff Report')
        colDiffGroup = finalMissMatch.groupBy(colDiffReq).count()
        rankColumnDiffTop = colDiffGroup.withColumn("row", row_number().over(
            Window.partitionBy("colName").orderBy(desc("count")))).filter(col("row") < 100).drop("row")
        rankColumnDiffTop.coalesce(1).write.mode("overwrite").option("header", "true").format(
            "com.databricks.spark.csv").save(hdfsOutLocation + '/MismatchCount')
        colDiffCountCheck = colDiffGroup.groupBy("colName").sum("count")
        colDiffCountCheck.coalesce(1).write.mode("overwrite").option("header", "true").format(
            "com.databricks.spark.csv").save(hdfsOutLocation + '/ColumnDiffSum')
        # finalMisMatchFiltered = finalMissMatch.withColumn("row", row_number().over(Window.partitionBy("colName").orderBy(primaryKey))).filter(col("row") < 1000).drop("row")
        # this function will get top 15 records in each column combinations
        finalMisMatchFiltered = finalMissMatch.withColumn("row", row_number().over(
            Window.partitionBy(colDiffReq).orderBy(primaryKey))).filter(col("row") < 25)
        finalMisMatchFiltered.coalesce(1).write.mode("overwrite").option("header", "true").format(
            "com.databricks.spark.csv").save(hdfsOutLocation + '/MismatchFiltered1000')
        del finalMissMatch, colDiffGroup, colDiffCountCheck, rankColumnDiffTop, finalMisMatchFiltered
    else:
        del finalMissMatch

if __name__ == '__main__':
    numeric_level = getattr(logging, 'INFO')
    logging.basicConfig(format='%(message)s', level=numeric_level)
    start_time = timeit.default_timer()
    logging.info('PySpark Compare Start')
    logging.info('Base Query = {}'.format(baseQuery))
    logging.info('Actual Query = {}'.format(actualQuery))
    logging.info('Output hdfs Location  = {}'.format(hdfsOutLocation))
    primaryKey = splitValuesByComma(keys.strip())
    consolidateData = "colName, baseline_data, actual_data"
    colDiffReq = splitValuesByComma(consolidateData.strip())
    logging.info('Primary Keys  = {}'.format(keys))
    ignoreColumns = splitValuesByComma(columnsToIgnore.strip())
    logging.info('Ignored Columns  = {}'.format(columnsToIgnore))
    baseDf, actualDf, matchedColumns = highLevelDetails(baseQuery, actualQuery, ignoreColumns)
    duplicateChecker(baseDf, actualDf, primaryKey)
    baseExtraCount, actualExtraCount = extraDataChecker(hdfsOutLocation, primaryKey)
    baseDf, actualDf = dropDuplicates(baseDf, actualDf, primaryKey)
    finalMissMatch = pysparkCompare(baseDf, actualDf, matchedColumns, primaryKey)
    writeMismatchToFile(primaryKey, finalMissMatch, hdfsOutLocation, colDiffReq, fullDiffRequired)
    elapsed_time = timeit.default_timer() - start_time
    timeTaken = str(datetime.timedelta(seconds=elapsed_time))
    mismatchCount = finalMissMatch.count()
    if mismatchCount == 0 and baseExtraCount == 0 and actualExtraCount == 0:
        logging.info('No Difference observed between actual and base')
    else:
        logging.info('Differences observed between actual and base')
    passedColumns = []
    for columnNameI in matchedColumns:
        if columnNameI not in ignoreColumns:
            passedColumns.append(columnNameI)
    logging.info(('Comparison completed in {}'.format(timeTaken)))
    logging.info('PySpark Compare End')
    logging.info('Passed Columns = {}'.format(passedColumns))