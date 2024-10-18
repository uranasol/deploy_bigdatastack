from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

your_bucket = "bdok-345594584978"
new_bucket = "newbdok-345594584978"

# set conf
conf = (
SparkConf()
    .set("spark.cores.max", "2")
        .set("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        .set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
        .set("spark.hadoop.fs.s3a.fast.upload", True)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.fs.s3a.aws.crendentials.provider", "com.amazonaws.auth.EnvironmentVariablesCredentials")
        .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3")
)

default_args = {
    'owner': 'uranasol',
    'start_date': datetime(2024, 2, 12)
}

@dag(
        default_args=default_args, 
        schedule_interval="@once", 
        description="Test out bigdata stack", 
        catchup=False, 
        tags=['Test']
)
def titanic_processing():

    # Task Definition
    start = DummyOperator(task_id='start')

    @task
    def first_task():
        print("Test begin!")
        spark = SparkSession\
        .builder\
        .appName("SparkApplicationJob")\
        .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

    @task
    def read_data():
        df = (
            spark
            .read
            .format("csv")
            .options(header='true', inferSchema='true', delimiter=';')
            .load("s3a://" + your_bucket + "/titanic.csv")
        )
        survivors = df.loc[df.Survived == 1, "Survived"].sum()
        survivors_sex = df.loc[df.Survived == 1, ["Survived", "Sex"]].groupby("Sex").count()
        return {
            'survivors_count': survivors,
            'survivors_sex': survivors_sex
        }

    @task
    def print_survivors(source):
        print(source['survivors_count'])

    @task
    def survivors_sex(source):
        print(source['survivors_sex'])

    last = BashOperator(
        task_id="last_task",
        bash_command='echo "This is the last task performed with Bash."',
    )

    end = DummyOperator(task_id='end')

    # Orchestration
    first = first_task()
    downloaded = read_data()
    start >> first >> downloaded
    surv_count = print_survivors(downloaded)
    surv_sex = survivors_sex(downloaded)

    [surv_count, surv_sex] >> last >> end

execution = titanic_processing()
