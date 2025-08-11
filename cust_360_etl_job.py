import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, month, year, dayofmonth, when, regexp_replace,sum
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#read files from data catalog
bronze_cust_profile_dyf = glueContext.create_dynamic_frame.from_catalog(
    database = 'cust_360_db',
    table_name = 'customer_profile_csv'
    )

bronze_cust_tran_dyf = glueContext.create_dynamic_frame.from_catalog(
    database = 'cust_360_db',
    table_name = 'customer_transcation_csv'
    )

# convert both dynamic frame to DF for transformation

cust_profile_df = bronze_cust_profile_dyf.toDF()
cust_tran_df = bronze_cust_tran_dyf.toDF()

# to print the sample of both table data
cust_tran_df.show(5)
cust_profile_df.show(5)

# count the record 

cust_profile_df.count()
cust_tran_df.count()

# remove duplicates from cust_profile table
remove_dup_df = cust_profile_df.dropDuplicates(['customer_id','signup_date'])
print(remove_dup_df.show())


# extract year, month date from cust_profile_df 
extract_date_df = remove_dup_df.withColumn('signup_year', year(col('signup_date')))\
                               .withColumn('signup_month', month(col('signup_date')))\
                               .withColumn('signup_day', dayofmonth(col('signup_date')))
print(extract_date_df.show())


# validate email ids

valid_email_df = extract_date_df.filter(col('email').rlike('.*\.(com|net)$'))
print(valid_email_df.show())


# writing this result to silver layer
cust_profile_enrich_df = valid_email_df.coalesce(1) \
    .write \
    .mode('overwrite') \
    .option('header', True) \
    .csv('s3://sneha-customer-360-bucket/silver_layer/cust_profile_enrich_data/')


# do the transformation on cust_tran_df

# remove duplicates from cust_trans table
remove_dup_tran_df = cust_tran_df.dropDuplicates(['transaction_date','customer_id'])
print(remove_dup_tran_df.show())

# generate is_large_transaction column 

is_large_tran_df = remove_dup_tran_df.withColumn(
    "is_large_transaction",
    when(col("amount").cast("double") > 1000, True).otherwise(False)
)

# remove special characters 
rm_spl_char_df = remove_dup_tran_df.withColumn('category', regexp_replace('category', '[^A-Za_z0-9]', ''))
print(
    rm_spl_char_df.select('category').distinct().show()
    )

# Amount validation
amt_vali_df = rm_spl_char_df.filter(
    (col("amount").cast("double") < 1000000) & 
    (col("amount").cast("double") > 0)
)
print(
     amt_vali_df.select('amount').distinct().show()
     )

# write data to liver layer
cust_transa_enrich_df = amt_vali_df.coalesce(1) \
    .write \
    .mode('overwrite') \
    .option('header', True) \
    .csv("s3://sneha-customer-360-bucket/silver_layer/cust_transa_enrich_data/")

# transform data from both cust_transa_enrich_df and cust_profile_enrich_df join both and perform gold layer aggregation 

# so need to create crawler on silver_layer and then read data from that table

# read data from catalog

cust_pro_dyf = glueContext.create_dynamic_frame.from_catalog(
    database = 'cust_360_db',
    table_name = 'cust_profile_enrich_data'
    )

cust_tr_dyf = glueContext.create_dynamic_frame.from_catalog(
    database = 'cust_360_db',
    table_name = 'cust_transa_enrich_data'
    )

# convert both dynamic frame to DF for transformation

cust_pro_df = cust_pro_dyf.toDF()
cust_tr_df = cust_tr_dyf.toDF()

# cast amount to double for aggregation
cust_tr_df = cust_tr_df.withColumn('amount',col('amount').cast('double'))

# join profile and transaction 

join_df = cust_tr_df.join(
    cust_pro_df, on='customer_id', how='inner'
    )
join_df.show()

# aggregation
# 1. total spending per customer
total_spend_df = cust_tr_df.groupBy('customer_id').agg(
    sum('amount').alias('total_spending')
    )
    
total_spend_df = total_spend_df.coalesce(1).write.mode('overwrite')\
.option('header','True')\
.csv("s3://sneha-customer-360-bucket/gold_layer/total_spend")

# top categories by spending
top_categories_df = cust_tr_df.groupBy('category').agg(
    sum('amount').alias('total_spent')
    ).orderBy(col('total_spent').desc())
    
top_categories_df = top_categories_df.coalesce(1).write.mode('overwrite')\
.option('header','True')\
.csv("s3://sneha-customer-360-bucket/gold_layer/top_categories")

# customer profile with spending
customer_profile_spend_df = join_df.groupBy(
    'customer_id','first_name','email','category'
    ).agg(
        sum('amount').alias('total_spending')
        )
customer_profile_spend_df = customer_profile_spend_df.coalesce(1).write.mode('overwrite')\
.option('header','True')\
.csv("s3://sneha-customer-360-bucket/gold_layer/customer_profile_spend")

job.commit()