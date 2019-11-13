import configparser

# Using config file to store AWS credentials and cluster details
# Using configparser library to extract the config details to be used within ETL process 
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLE STATEMENTS
drop_demo_total = 'DROP TABLE IF EXISTS state_demographics;'
drop_demo_race = 'DROP TABLE IF EXISTS state_demographics_by_race;'
drop_visitors_analysis = 'DROP TABLE IF EXISTS visitor_analysis;'
drop_stage_visitors_analysis = 'DROP TABLE IF EXISTS stage_visitor_analysis;'
drop_visitors = 'DROP TABLE IF EXISTS visitors;'
drop_stage_visitors = 'DROP TABLE IF EXISTS stage_visitors;'
drop_arrival_date = 'DROP TABLE IF EXISTS arrival_date;'
drop_stage_arrival_date = 'DROP TABLE IF EXISTS stage_arrival_date;'


# TRUNCATE TABLE
# TRUNCATE command may commit other operations when it commits itself
truncate_demo_total = 'TRUNCATE TABLE state_demographics;'
truncate_demo_race = 'TRUNCATE TABLE state_demographics_by_race;'
truncate_stage_visitors_analysis = 'TRUNCATE TABLE stage_visitor_analysis;'
truncate_stage_visitors = 'TRUNCATE TABLE stage_visitors;'
truncate_stage_arrival_date = 'TRUNCATE TABLE stage_arrival_date;'


# CREATE TABLE STATEMENTS
create_demo_total = ("""CREATE TABLE state_demographics(city                   VARCHAR(256) NOT NULL
                                                    , state_code             CHAR(2)      NOT NULL
                                                    , state                  VARCHAR(256)
                                                    , median_age             NUMERIC(3,1)
                                                    , male_population        BIGINT
                                                    , female_population      BIGINT
                                                    , total_population       BIGINT       NOT NULL
                                                    , number_of_veterans     BIGINT
                                                    , foreign_born           BIGINT
                                                    , average_household_size NUMERIC(4,2)
                                                    , CONSTRAINT tot_pop_pkey PRIMARY KEY (city, state_code))
                        DISTSTYLE ALL
                        SORTKEY (state_code, city);
                     """)

create_demo_race = ("""CREATE TABLE state_demographics_by_race(city         VARCHAR(256) NOT NULL
                                                  , state_code   CHAR(2)      NOT NULL
                                                  , state        VARCHAR(256)
                                                  , race         VARCHAR(256)
                                                  , count        BIGINT       NOT NULL
                                                  , CONSTRAINT race_pkey PRIMARY KEY (city, state_code, race))
                        DISTSTYLE ALL
                        SORTKEY (state_code, city);
                     """)

create_stage_visitor_analysis = ("""CREATE TABLE stage_visitor_analysis(
                                                            admission_id         BIGINT       
                                                          , year_month           INT          
                                                          , arrival_date         DATE         
                                                          , dest_state_code      VARCHAR(10)
                                                          , port_city            VARCHAR(256)
                                                          , port_state_code      VARCHAR(10)
                                                          , entry_mode           VARCHAR(10)
                                                          , visa_category        VARCHAR(20)
                                                          , origin_country       VARCHAR(256)
                                                          , port_dest_equal_flag CHAR(1))
                                    BACKUP NO;
                                 """)

create_visitor_analysis = ("""CREATE TABLE visitor_analysis(admission_id         BIGINT       NOT NULL
                                                          , year_month           INT          NOT NULL DISTKEY SORTKEY
                                                          , arrival_date         DATE         NOT NULL
                                                          , dest_state_code      VARCHAR(10)
                                                          , port_city            VARCHAR(256)
                                                          , port_state_code      VARCHAR(10)
                                                          , entry_mode           VARCHAR(10)
                                                          , visa_category        VARCHAR(20)
                                                          , origin_country       VARCHAR(256)
                                                          , port_dest_equal_flag CHAR(1)
                                                          , CONSTRAINT vis_ana_pkey PRIMARY KEY (admission_id, arrival_date))
                              DISTSTYLE KEY;
                           """)

create_stage_visitors = ("""CREATE TABLE stage_visitors(
                                            admission_id        BIGINT       
                                          , year_month          INT          
                                          , origin_country      VARCHAR(256)
                                          , dest_state_code     VARCHAR(10)
                                          , port_city           VARCHAR(256)
                                          , port_state_code     VARCHAR(10)
                                          , entry_mode          VARCHAR(10)
                                          , visitor_age         INT
                                          , visitor_birth_year  INT
                                          , arrival_date        DATE         
                                          , departure_date      DATE
                                          , visa_category       VARCHAR(20)
                                          , visatype            VARCHAR(10)
                                          , visa_expire_date    VARCHAR(20)
                                          , gender              VARCHAR(10))
                      BACKUP NO;
                   """)

create_visitors = ("""CREATE TABLE visitors(admission_id        BIGINT       NOT NULL
                                          , year_month          INT          NOT NULL DISTKEY SORTKEY
                                          , origin_country      VARCHAR(256)
                                          , dest_state_code     VARCHAR(10)
                                          , port_city           VARCHAR(256)
                                          , port_state_code     VARCHAR(10)
                                          , entry_mode          VARCHAR(10)
                                          , visitor_age         INT
                                          , visitor_birth_year  INT
                                          , arrival_date        DATE         NOT NULL
                                          , departure_date      DATE
                                          , visa_category       VARCHAR(20)
                                          , visatype            VARCHAR(10)
                                          , visa_expire_date    VARCHAR(20)
                                          , gender              VARCHAR(10)
                                          , CONSTRAINT visitors_pkey PRIMARY KEY (admission_id, arrival_date))
                      DISTSTYLE KEY;
                   """)
                   
create_stage_arrival_date = ("""CREATE TABLE stage_arrival_date(
                                                    arrival_date    DATE
                                                  , "year"          INT
                                                  , "month"         INT
                                                  , "day"           INT
                                                  , weekday         INT
                                                  , weeknum         INT)
                                BACKUP NO;
                             """)

create_arrival_date = ("""CREATE TABLE arrival_date(arrival_date    DATE NOT NULL PRIMARY KEY
                                                  , "year"          INT
                                                  , "month"         INT
                                                  , "day"           INT
                                                  , weekday         INT
                                                  , weeknum         INT)
                          DISTSTYLE ALL
                          SORTKEY (year, month);
                       """)
                       
# COPY STATEMENTS
copy_demo_total = ("""COPY state_demographics FROM '{}'
                      CREDENTIALS 'aws_iam_role={}'
                      FORMAT AS JSON 'auto'
                      region 'us-west-2';
                   """).format(config['S3']['DEMO_TOT'],config['IAM_ROLE']['ARN'])

copy_demo_race = ("""COPY state_demographics_by_race FROM '{}'
                     CREDENTIALS 'aws_iam_role={}'
                     FORMAT AS JSON 'auto'
                     region 'us-west-2';
                  """).format(config['S3']['DEMO_RACE'],config['IAM_ROLE']['ARN'])

copy_stage_visitor_analysis = ("""COPY stage_visitor_analysis FROM '{}'
                                  CREDENTIALS 'aws_iam_role={}'
                                  FORMAT AS JSON 'auto'
                                  region 'us-west-2';
                               """).format(config['S3']['VISITOR_ANA'],config['IAM_ROLE']['ARN'])

copy_stage_visitors = ("""COPY stage_visitors FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS JSON 'auto'
                          region 'us-west-2';
                       """).format(config['S3']['VISITORS'],config['IAM_ROLE']['ARN'])

copy_stage_arr_date = ("""COPY stage_arrival_date FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS JSON 'auto'
                          region 'us-west-2';
                       """).format(config['S3']['ARR_DATE'],config['IAM_ROLE']['ARN'])

# INSERT STATEMENTS
insert_visitor_analysis = """INSERT INTO visitor_analysis(admission_id, year_month, arrival_date
                                  , dest_state_code, port_city, port_state_code, entry_mode
                                  , visa_category, origin_country, port_dest_equal_flag)
                             SELECT admission_id, year_month, arrival_date, dest_state_code
                                  , port_city, port_state_code, entry_mode, visa_category
                                  , origin_country, port_dest_equal_flag
                             FROM stage_visitor_analysis;
                          """
                          
insert_visitors = """INSERT INTO visitors(admission_id, year_month, origin_country, dest_state_code
                          , port_city, port_state_code, entry_mode, visitor_age, visitor_birth_year
                          , arrival_date, departure_date, visa_category, visatype
                          , visa_expire_date, gender)
                     SELECT admission_id, year_month, origin_country, dest_state_code, port_city
                           , port_state_code, entry_mode, visitor_age, visitor_birth_year
                           , arrival_date, departure_date, visa_category, visatype
                           , visa_expire_date, gender
                     FROM stage_visitors;
                  """

insert_arr_date = """INSERT INTO arrival_date(arrival_date, "year", "month", "day", weekday, weeknum)
                     SELECT arrival_date, "year", "month", "day", weekday, weeknum
                     FROM stage_arrival_date;
                  """
                    
# Data quality check queries
check_zero_count = ("""SELECT COUNT(*) FROM {};""")

check_unique_key = ("""SELECT COUNT({}) - COUNT(DISTINCT{}) FROM {};""")


# QUERY LISTS
create_table_queries = [create_demo_race, create_demo_total, create_arrival_date
                      , create_visitor_analysis, create_visitors, create_stage_arrival_date
                      , create_stage_visitor_analysis, create_stage_visitors]

drop_table_queries = [drop_demo_total, drop_demo_race, drop_visitors_analysis
                    , drop_visitors, drop_arrival_date, drop_stage_arrival_date
                    , drop_stage_visitors, drop_stage_visitors_analysis]

trunc_demo_table_queries = [truncate_demo_total, truncate_demo_race]

copy_demo_table_queries = [copy_demo_total, copy_demo_race]

trunc_immi_table_queries = [truncate_stage_arrival_date, truncate_stage_visitors
                          , truncate_stage_visitors_analysis]

copy_immi_table_queries = [copy_stage_visitor_analysis, copy_stage_visitors, copy_stage_arr_date]

insert_immi_table_queries = [insert_visitor_analysis, insert_visitors, insert_arr_date]