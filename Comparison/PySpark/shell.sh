#!/usr/bin/env bash

INIT_PATH="$( cd "$( dirname "$(readlink -f "${BASH_SOURCE[0]}")" )" && pwd )"
BASE_PATH=$(readlink -f $INIT_PATH/..)
BIN_PATH=$BASE_PATH/bin

echo "BASE_PATH = {BASE_PATH}"

#source $BIN_PATH/setup_env_variables.sh

if [ -z "$SPARK_HOME" ]; then
    echo "Warning: SPARK_HOME should point to your Apache Spark installation!"
fi

$SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --keytab /home/nameqa.keytab \
    --principal location \
    --queue queue_name \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PY3ENV/py36env/bin/python3 \
    --conf spark.yarn.maxAppAttempts=1 \
    --conf spark.executor.memory=16g \
    --conf spark.executor.cores=7 \
    --conf spark.driver.cores=7 \
    --conf spark.driver.maxResultSize=16g \
    --conf spark.driver.memory=16g \
    --conf spark.ui.view.acls=* \
    --conf spark.ui.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.initialExecutors=7 \
    --conf spark.dynamicAllocation.maxExecutors=25 \
    --archives /tmp/Automation_logs/py36env.zip#PY3ENV \
    --py-files [INPUT_PYTHON_FILE_LOCATION] \
    $1

export EXIT_CODE=$?
echo "Exiting with ${EXIT_CODE}"