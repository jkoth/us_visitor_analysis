##########################################################################################################
nohup \
spark-submit --master yarn \
             --packages 'org.apache.hadoop:hadoop-aws:2.8.5' \
             --conf spark.hadoop.fs.s3a.fast.upload=true \       # Skips local buffer before writing to S3
             demographics_data_processing.py \
1>log 2>&1 &

##########################################################################################################
import os
from sys import exit
from logging import error
from pyspark.sql import SparkSession                    # Main entry point for DataFrame and SQL functionality

"""
Purpose:
  - Instantiates or returns existing SparkSession() with given properties
  - JARs required in the script are supplied in spark-submit
"""
def create_spark_session():
    spark = SparkSession.builder \
                        .appName('Demographics_Data_Processing') \
                        .getOrCreate()
    return spark


"""
Purpose:
  - Read demographics JSON data files into Spark DataFrame
  - Extract required data elements stored in a nested JSON field, 'fields'
  - Drop unrequired attributes from DF
  - Split data into two DF, total population and race details
      ~ Race attribute causes redundancies in population stats
      ~ Spliting data into two tables makes aggregations easier for end-user
  - Write transformed data to given directory in JSON format
Param:
  - @spark: Spark DF instance
  - @read_dir: Highest directory name where data is stored; Bucket name for S3
  - @write_dir: Highest directory name where data needs to be stored after transformation
"""
def process_demographics_data(spark, read_dir, write_dir):
    
    read_file='demographic_data/'                              #directory name where demographic data is stored
    read_file_path=os.path.join(read_dir, read_file)

    # Create Spark DataFrame for demographic data
    demographic_df = spark.read.json(read_file_path)

    # Extract fields from nested JSON
    # Drop unwanted columns
    demographic_df = demographic_df.withColumn('average_household_size', demographic_df.fields.average_household_size) \
                                   .withColumn('city', demographic_df.fields.city) \
                                   .withColumn('count', demographic_df.fields.count) \
                                   .withColumn('female_population', demographic_df.fields.female_population) \
                                   .withColumn('foreign_born', demographic_df.fields.foreign_born) \
                                   .withColumn('male_population', demographic_df.fields.male_population) \
                                   .withColumn('median_age', demographic_df.fields.median_age) \
                                   .withColumn('number_of_veterans', demographic_df.fields.number_of_veterans) \
                                   .withColumn('race', demographic_df.fields.race) \
                                   .withColumn('state', demographic_df.fields.state) \
                                   .withColumn('state_code', demographic_df.fields.state_code) \
                                   .withColumn('total_population', demographic_df.fields.total_population) \
                                   .drop('record_timestamp','recordid','datasetid','fields')

    # Demographics without race details
    df_demo_population = demographic_df.select('city','state_code','state','median_age','male_population'
                                             , 'female_population','total_population','number_of_veterans'
                                             , 'foreign_born','average_household_size') \
                                       .drop_duplicates()

    # Write out transformed data in JSON file format
    write_path=os.path.join(write_dir, 'demo_total_pop_dim_table/')
    df_demo_population.write.json(write_path, mode='overwrite')

    # Demographics race details
    df_demo_race = demographic_df.select('city','state_code','state','race','count') \
                                 .drop_duplicates()
                                        
    # Write out transformed data in JSON file format
    write_path=os.path.join(write_dir, 'demo_race_pop_dim_table/')
    df_demo_race.write.json(write_path, mode='overwrite')


"""
Purpose:
  - Instantiate SparkSession() by calling create_spark_session function and stop after processing data
  - Call process_demographics_data to extract, transform, and load demographic data
"""
def main():
    # AWS S3 paths for input and output data storage
    main_dir = 's3a://capstone-ndde/'                                          
    output_dir = os.path.join(main_dir, 'visitor_analysis_dw/')                

    # Instantiate SparkSession()
    try:
        spark = create_spark_session()
    except Exception as e:
        error(f"Error creating SparkSession: {e}")
        spark.stop()
        exit()
    
    # Call process_demographics_data function
    try:
        process_demographics_data(spark, main_dir, output_dir)
    except Exception as e:
        error(f"Error processing demographics data: {e}")
        spark.stop()
        exit()
    
    # Stop SparkSession
    spark.stop()

"""
  - Run above code if the file is labled __main__
  - Python internally labels files at runtime to differentiate between imported files and main file
"""
if __name__ == "__main__":
    main()