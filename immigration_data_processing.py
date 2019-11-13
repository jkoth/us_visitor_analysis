##########################################################################################################
# Pass immigration file name as argument
# SAS format expects individual file names; can't accept directory only
nohup \
spark-submit --master yarn \
             --packages 'org.apache.hadoop:hadoop-aws:2.8.5' \
             --packages 'saurfang:spark-sas7bdat:2.0.0-s_2.11' \
             --conf spark.hadoop.fs.s3a.fast.upload=true \
             immigration_data_processing.py 'i94_feb16_sub.sas7bdat' \
1>log 2>&1 &

##########################################################################################################
import os                                               
from sys import exit, argv                              
from logging import error                               
from pyspark.sql import SparkSession                    # Main entry point for DataFrame and SQL functionality
import pyspark.sql.types as Spark_DT                    # Spark DataTypes
from datetime import datetime, timedelta                # Used for Date & Time related operations
import pyspark.sql.functions as F                       # PySpark functions used in below script

"""
Purpose:
  - Instantiates or returns existing SparkSession() with given properties
  - JARs required in the script are supplied in spark-submit
"""
def create_spark_session():
    spark = SparkSession.builder \
                        .appName('Immigration_Data_Processing') \
                        .getOrCreate()
    return spark


"""
Purpose:
  - Read immigration SAS data file into Spark DataFrame
  - Read csv formatted mapping data files as Spark DataFrames
  - Perform data cleaning:
      ~ Change data types from FLOAT to INT/BIGINT, as needed
      ~ Drop records without valid adminnum value
      ~ Fill NULLs with default values
      ~ Convert SAS Numeric date field to Spark DateType
      ~ Decode attributes using mapping DFs
      ~ Drop duplicates
  - Create visitors and visitors_analysis DFs
  - Write transformed data to given directory in JSON format
Param:
  - @spark: Spark DF instance
  - @read_dir: Highest directory name where data is stored; Bucket name for S3
  - @write_dir: Highest directory name where data needs to be stored after transformation
  - @immigration_file_name: Immigration file to be processed  
"""
def process_immigration_data(spark, read_dir, write_dir, immigration_file_name):
    
    # Create DataFrame for port mapping data
    read_file='transformation_data/I94-Port-Decode.csv'
    read_file_path=os.path.join(read_dir, read_file)
    df_port = spark.read.csv(read_file_path, header=True
                            , ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)

    # Create DataFrame for US address mapping data
    read_file='transformation_data/I94-ADDR-Decode.csv'
    read_file_path=os.path.join(read_dir, read_file)
    df_addr = spark.read.csv(read_file_path, header=True
                            , ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)
                            
    # Create DataFrame for origin country mapping data
    read_file='transformation_data/I94-Origin-Country-Decode.csv'
    read_file_path=os.path.join(read_dir, read_file)
    df_org_cntry = spark.read.csv(read_file_path, header=True
                            , ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)

    # Create DataFrame for immigration data
    read_file=os.path.join('immigration_data/', immigration_file_name)
    read_file_path=os.path.join(read_dir, read_file)
    df_im =spark.read.format('com.github.saurfang.sas.spark').load(read_file_path)
    
    # Data Cleaning
    # Change float data type to int/bigint to match final DW DT
    # Drop duplicates
    df_im = df_im.select(df_im['admnum'].cast('bigint')
                       , df_im['i94yr'].cast('int')
                       , df_im['i94mon'].cast('int')
                       , df_im['i94cit'].cast('int')
                       , df_im['i94addr']
                       , df_im['i94port']
                       , df_im['arrdate'].cast('int')
                       , df_im['depdate'].cast('int')
                       , df_im['i94mode'].cast('int')
                       , df_im['i94bir'].cast('int')
                       , df_im['biryear'].cast('int')
                       , df_im['i94visa'].cast('int')
                       , df_im['visatype']
                       , df_im['dtaddto']
                       , df_im['gender']) \
                 .drop_duplicates()

    # Drop records without admission_id
    df_im = df_im.dropna(subset='admnum')

    # Fill NULL fields with default values
    # arrdate, depdate fields contain day counts since SAS epoch Jan 1, 1960
    #   These columns will be converted from SAS to Spark date type using udf
    #   NULL values will be defaulted to 0 days
    df_im = df_im.fillna(-1, subset=['i94bir','biryear']) \
                 .fillna(0, subset=['arrdate','depdate']) \
                 .fillna('N/R', subset=['visatype','gender','dtaddto'])
                 
    # Decode port field into port city and port state using mapping DF
    df_im = df_im.join(df_port, df_im.i94port == df_port.port_code, 'left').drop(df_port.port_code)

    # Decode visitor's US residence State field using mapping DF
    df_im = df_im.join(df_addr, df_im.i94addr == df_addr.dest_state_code, 'left')

    # Decode visitor's origin country field using mapping DF
    df_im = df_im.join(df_org_cntry, df_im.i94cit == df_org_cntry.origin_code, 'left') \
                 .drop(df_org_cntry.origin_code)

    # Fill NULL fields resulting from joins with default value, 'Other'
    df_im = df_im.fillna('Other', subset=['port_city','port_state_code','dest_state_code'
                                        , 'dest_state_name','origin_country'])

    # UDF to Convert SAS numeric field to Spark DateType
    sas_to_spark_dt = F.udf(lambda sas_days: datetime(1960, 1, 1) + timedelta(days = sas_days), Spark_DT.DateType())
                          
    # create new immigration DF with transformed data
    df_im = df_im.select(df_im['admnum'].alias('admission_id')
                       , F.concat(df_im['i94yr'], df_im['i94mon']).alias('year_month')
                       , df_im['i94cit']
                       , df_im['i94addr']
                       , df_im['i94port']
                       , sas_to_spark_dt(df_im['arrdate']).alias('arrival_date')
                       , sas_to_spark_dt(df_im['depdate']).alias('departure_date')
                       , df_im['i94bir'].alias('visitor_age')
                       , df_im['biryear'].alias('visitor_birth_year')
                       , df_im['visatype']
                       , df_im['gender']
                       , df_im['port_city']
                       , df_im['port_state_code']
                       , df_im['dest_state_code']
                       , df_im['dest_state_name']
                       , df_im['origin_country']
                       , F.when(df_im.i94mode == 1, 'Air').when(df_im.i94mode == 2, 'Sea') \
                          .when(df_im.i94mode == 3, 'Land').otherwise('N/R') \
                          .alias('entry_mode')
                       , F.when(df_im.i94visa == 1, 'Business').when(df_im.i94visa == 2, 'Pleasure') \
                          .when(df_im.i94visa == 3, 'Student').otherwise('N/R') \
                          .alias('visa_category')
                       , df_im['dtaddto'].alias('visa_expire_date')
                       , F.when(df_im['dest_state_code'] == df_im['port_state_code'], 'Y') \
                          .otherwise('N').alias('port_dest_equal_flag'))

    # Visitors Analysis Fact table
    visitor_analysis = df_im.select('admission_id', 'year_month' , 'arrival_date', 'dest_state_code'
                                  , 'port_city', 'port_state_code', 'entry_mode', 'visa_category'
                                  , 'origin_country', 'port_dest_equal_flag')
                                  
    # Write out visitors_analysis fact table in JSON format
    write_path = os.path.join(write_dir, 'visitor_analysis_fact_table/')
    visitor_analysis.write.json(write_path, mode='overwrite')

    # Visitors dim table
    visitors = df_im.select('admission_id', 'year_month', 'origin_country'
                          , 'dest_state_code', 'port_city', 'port_state_code'
                          , 'entry_mode', 'visitor_age', 'visitor_birth_year'
                          , 'arrival_date', 'departure_date', 'visa_category'
                          , 'visatype', 'visa_expire_date', 'gender')

    # Write out visitors dim table in JSON format
    write_path = os.path.join(write_dir, 'visitors_dim_table/')
    visitors.write.json(write_path, mode='overwrite')
    
    # Arrival Date dim table
    arrival_date = df_im.select(df_im['arrival_date']
                              , F.year(df_im['arrival_date']).alias('year')
                              , F.month(df_im['arrival_date']).alias('month')
                              , F.dayofmonth(df_im['arrival_date']).alias('day')
                              , F.dayofweek(df_im['arrival_date']).alias('weekday')
                              , F.weekofyear(df_im['arrival_date']).alias('weeknum')
                                ) \
                        .drop_duplicates()

    # Write out visitors dim table in JSON format
    write_path = os.path.join(write_dir, 'arrival_date_dim_table/')
    arrival_date.write.json(write_path, mode='overwrite')

"""
Purpose:
  - Instantiate SparkSession() by calling create_spark_session function and stop after processing data
  - Call process_immigration_data to extract, transform, and load immigration data
"""
def main():
    # AWS S3 paths for input and output data storage
    main_dir = 's3a://capstone-ndde/'                                          # To source raw data
    output_dir = os.path.join(main_dir, 'visitor_analysis_dw/')                # To store transformed DWH tables
    immigration_file = argv[1]                                                 # Take file name from command args

    # Instantiate SparkSession()
    try:
        spark = create_spark_session()
    except Exception as e:
        error(f"Error creating SparkSession: {e}")
        spark.stop()
        exit()
    
    # Call process_immigration_data function
    try:
        process_immigration_data(spark, main_dir, output_dir, immigration_file)
    except Exception as e:
        error(f"Error processing immigration data: {e}")
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