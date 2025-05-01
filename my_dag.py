from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
import pyspark.sql.functions as F

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def get_spark_session():
    return SparkSession.builder \
        .appName("Monthly ETL") \
        .config("spark.jars", "/home/ubuntu/postgresql-42.7.5.jar") \
        .enableHiveSupport() \
        .getOrCreate()

def load_month_data(**kwargs):
    execution_date = kwargs['execution_date']
    spark = get_spark_session()

    prev_date = execution_date - relativedelta(months=1)
    year = prev_date.year
    month = prev_date.month

    # Чтение из HDFS
    df = spark.read.option("header", True).csv("hdfs:///user/ubuntu/nashville_accidents/nashville_accidents_2018_2025.csv")

    df = df.withColumn('Date and Time', F.to_timestamp('Date and Time', 'M/d/yyyy h:mm:ss a'))
    df = df \
        .withColumnRenamed('Accident Number', 'accident_number') \
        .withColumnRenamed('Date and Time', 'date') \
        .withColumnRenamed('Number of Motor Vehicles', 'number_of_motor_vehicles') \
        .withColumnRenamed('Number of Injuries', 'number_of_injuries') \
        .withColumnRenamed('Number of Fatalities', 'number_of_fatalities') \
        .withColumnRenamed('Property Damage', 'property_damage') \
        .withColumnRenamed('Hit and Run', 'hit_and_run') \
        .withColumnRenamed('Collision Type Description', 'collision_type_description') \
        .withColumnRenamed('Weather Description', 'weather_description') \
        .withColumnRenamed('Illumination Description', 'illumination_description') \
        .withColumnRenamed('Street Address', 'street_address') \
        .withColumnRenamed('City', 'city') \
        .withColumnRenamed('State', 'state') \
        .withColumnRenamed('Precinct', 'precinct') \
        .withColumnRenamed('Lat', 'lat') \
        .withColumnRenamed('Long', 'long') \
        .withColumnRenamed('HarmfulCodes', 'harmful_codes') \
        .withColumnRenamed('HarmfulDescriptions', 'harmful_descriptions') \
        .withColumnRenamed('ObjectId', 'object_id') \
        .withColumnRenamed('Zip Code', 'zip_code') \
        .withColumnRenamed('RPA', 'rpa') \
        .withColumnRenamed('Weather', 'weather') \
        .withColumnRenamed('Collision Type', 'collision_type') \
        .withColumnRenamed('Reporting Officer', 'reporting_officer')

    monthly_df = df.filter((F.year("date") == year) & (F.month("date") == month))
    monthly_df.write.mode("overwrite").format("orc").saveAsTable("etl_project_db.monthly_data_temp")

def update_hive_tables(**kwargs):
    spark = get_spark_session()

    spark.sql("CREATE DATABASE IF NOT EXISTS etl_project_db")

    df = spark.table("etl_project_db.monthly_data_temp")

    # First table
    first_table = df.select(
        "accident_number",
        "date",
        "state",
        "number_of_injuries",
        "number_of_fatalities"
    )

    first_table = first_table \
        .withColumn("accident_number", when(col("accident_number").rlike("^[0-9]+$"), col("accident_number")).cast("bigint")) \
        .withColumn("number_of_injuries", when(col("number_of_injuries").rlike("^[0-9]+$"), col("number_of_injuries")).cast("int")) \
        .withColumn("number_of_fatalities", when(col("number_of_fatalities").rlike("^[0-9]+$"), col("number_of_fatalities")).cast("int"))

    first_table.write.mode("append").format("orc").saveAsTable("etl_project_db.first_table")

    # Second table
    second_table = df.select(
        "accident_number",
        "date",
        "state",
        "city",
        "street_address",
        "lat",
        "long",
        "number_of_motor_vehicles"
    )

    second_table = second_table \
        .withColumn("accident_number", when(col("accident_number").rlike("^[0-9]+$"), col("accident_number")).cast("bigint")) \
        .withColumn("lat", when(col("lat").rlike("^[0-9]+$"), col("lat")).cast("double")) \
        .withColumn("long", when(col("long").rlike("^[0-9]+$"), col("long")).cast("double")) \
        .withColumn("number_of_motor_vehicles", when(col("number_of_motor_vehicles").rlike("^[0-9]+$"), col("number_of_motor_vehicles")).cast("int"))

    second_table.write.mode("append").format("orc").saveAsTable("etl_project_db.second_table")

    # Third table
    third_table = df.select(
        "accident_number",
        "date",
        "reporting_officer",
        "property_damage",
        "hit_and_run",
        "collision_type_description",
        "weather_description",
        "illumination_description"
    )

    third_table = third_table \
        .withColumn("accident_number", when(col("accident_number").rlike("^[0-9]+$"), col("accident_number")).cast("bigint")) \
        .withColumn("reporting_officer", when(col("reporting_officer").rlike("^[0-9]+$"), col("reporting_officer")).cast("bigint")) \
        .withColumn("property_damage", when(col("property_damage").rlike("^[0-9]+$"), col("property_damage")).cast("int"))

    third_table.write.mode("append").format("orc").saveAsTable("etl_project_db.third_table")

def replicate_to_postgres(**kwargs):
    execution_date = kwargs['execution_date']
    spark = get_spark_session()

    prev_date = execution_date - relativedelta(months=1)
    year = prev_date.year
    month = prev_date.month

    jdbc_url = "jdbc:postgresql://158.160.133.10:5432/mydatabase"
    props = {"user": "myuser", "password": "mypassword", "driver": "org.postgresql.Driver"}

    # First table
    df1 = spark.table("etl_project_db.first_table")
    agg1 = df1.filter((F.year("date") == year) & (F.month("date") == month)) \
        .groupBy(
            F.year('date').alias('year'),
            F.month('date').alias('month'),
            'state'
        ).agg(
            F.count("number_of_injuries").alias("injuries_count"),
            F.count("number_of_fatalities").alias("fatalities_count")
    )

    agg1.write.jdbc(url=jdbc_url, table="injuries_and_fatalities_per_month", mode="overwrite", properties=props)

    # Second table
    df2 = spark.table("etl_project_db.second_table")
    agg2 = df2.filter((F.year("date") == year) & (F.month("date") == month)) \
        .groupBy(
            F.year('date').alias('year'),
            F.month('date').alias('month'),
            'city'
        ).agg(
            F.count('number_of_motor_vehicles').alias('count_vehicles')
        )

    agg2.write.jdbc(url=jdbc_url, table="number_of_motor_vehicles_per_month", mode="overwrite", properties=props)

    # Third table
    df3 = spark.table("etl_project_db.third_table")
    df3 = df3.withColumn('column', F.lit(1))
    agg3 = df3.filter((F.year("date") == year) & (F.month("date") == month)) \
        .groupBy(
            F.year('date').alias('year'),
            F.month('date').alias('month'),
            'weather_description'
        ).agg(
            F.count('column').alias('count_weather_type')
        )

    agg3.write.jdbc(url=jdbc_url, table="weather_type_count_per_month", mode="overwrite", properties=props)


with DAG(
    'monthly_etl_hive_postgres',
    default_args=default_args,
    description='ETL: HDFS -> Hive -> Postgres по месяцам',
    schedule_interval='@monthly',
    start_date=datetime(2018, 2, 1),
    catchup=True,
    max_active_runs=3,
    tags=['etl', 'spark', 'postgres', 'hive']
) as dag:

    task_load = PythonOperator(
        task_id='load_month_data',
        python_callable=load_month_data,
        provide_context=True
    )

    task_hive = PythonOperator(
        task_id='update_hive_tables',
        python_callable=update_hive_tables,
        provide_context=True
    )

    task_pg = PythonOperator(
        task_id='replicate_to_postgres',
        python_callable=replicate_to_postgres,
        provide_context=True
    )

    task_load >> task_hive >> task_pg