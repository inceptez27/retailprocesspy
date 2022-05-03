from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from datetime import datetime

def stageprocess(spark,prop):

    #create database
    spark.sql("DROP DATABASE IF EXISTS retail_stg CASCADE")
    spark.sql("create database if not exists retail_stg  location '/user/hive/warehouse/retail_stg'")
    
    if (prop.get("srctype").strip() == "S3"):
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", prop.get("fs.s3a.awsAccessKeyId"))
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", prop.get("fs.s3a.awsSecretAccessKey"))
        spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider") 
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
  
    
    
    
    print("======staging process started at " , datetime.now())
    
    #Read customer data from csv file and load into tblcustomer_stg under database retail_stg
    readfileandwriteintostaging(spark,prop.get("srccustomer"),"retail_stg.tblcustomer_stg")
    
    #Read product category data from csv file and load into tblproductcategory_stg under database retail_stg
    readfileandwriteintostaging(spark,prop.get("srcproductcategory"),"retail_stg.tblproductcategory_stg")
    
    readfileandwriteintostaging(spark,prop.get("srcproductsubcategory"),"retail_stg.tblproductsubcategory_stg")
    
    readfileandwriteintostaging(spark,prop.get("srcsales"),"retail_stg.tblsales_stg")
    
    readfileandwriteintostaging(spark,prop.get("srcterritory"),"retail_stg.tblterritory_stg")
    
    readfileandwriteintostaging(spark,prop.get("srcproduct"),"retail_stg.tblproduct_stg")
    print("======staging process completed at " , datetime.now())
    


def readfileandwriteintostaging(spark,filename,tablename):
    print("load data into hive data: " + tablename)
    df = spark.read.format("csv") \
    .option("header",True) \
    .option("inferSchema",True) \
    .load(filename)
    
    
      
    df1 = df.withColumn("load_dt", current_date())
    
    df1.show()  
      
    #write data into hive table
    df1.write.mode("overwrite").saveAsTable(tablename)
    print("written data into hive table:" + tablename)
    



   