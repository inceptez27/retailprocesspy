from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date,to_date,regexp_replace
from datetime import datetime

def curateprocess(spark,prop):
    spark.sql("DROP DATABASE IF EXISTS retail_curated CASCADE")
    spark.sql("create database if not exists retail_curated location '/user/hive/warehouse/retail_curated'")
    print("====curation process started at " , datetime.now())
    
    dfproduct = spark.sql("""select p.ProductKey,p.ProductName,p.ModelName,p.ProductDescription,p.ProductColor,p.ProductSize,p.ProductStyle,p.ProductCost,p.ProductPrice,
    pc.CategoryName,ps.SubcategoryName,p.load_dt from retail_stg.tblproduct_stg p inner join retail_stg.tblproductsubcategory_stg ps on p.ProductSubcategoryKey=ps.ProductSubcategoryKey
    inner join retail_stg.tblproductcategory_stg pc on ps.ProductCategoryKey = pc.ProductCategoryKey""")
    
    dfproduct1 = dfproduct.withColumn("profit", dfproduct["ProductPrice"] - dfproduct["ProductCost"])
    
    dfproduct1.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblproduct_dtl")
    
    print("=== data written into product table in curated database=======")
    
    dfcustomer = spark.sql("""select CustomerKey,Prefix,FirstName,LastName,BirthDate,MaritalStatus,Gender,EmailAddress,AnnualIncome,TotalChildren,
                                EducationLevel,Occupation,HomeOwner,load_dt from retail_stg.tblcustomer_stg""")
    
    dfcustomer1 = dfcustomer.withColumn("Birth_Date",to_date(dfcustomer["BirthDate"],"M/d/yyyy")) \
                                .withColumn("Annual_Income",regexp_replace(regexp_replace(regexp_replace("AnnualIncome"),"\\$",""),",","").cast("int")) \
                                .drop("BirthDate","AnnualIncome")
    
    dfcustomer1.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblcustomer_dtl")
    
    print("=== data written into customer table in curated database=======")
    
    #Sales data process
    dfsales = spark.sql("""select OrderDate,StockDate,OrderNumber,ProductKey,CustomerKey,TerritoryKey,OrderLineItem,OrderQuantity,load_dt from retail_stg.tblsales_stg""")
    
    dfsales1 = dfsales.withColumn("Order_Date",to_date(dfsales["OrderDate"],"M/d/yyyy")) \
     .withColumn("Stock_Date",to_date(dfsales["StockDate"],"M/d/yyyy")) \
     .drop("OrderDate","StockDate")
     
    dfsales1.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblsales_dtl")
    
    print("=== data written into sales table in curated database=======")
    
    
    dfterritory = spark.sql("""select SalesTerritoryKey,Region,Country,Continent,load_dt from retail_stg.tblterritory_stg""")
    dfterritory.write.mode("overwrite").partitionBy("load_dt").saveAsTable("retail_curated.tblterritory_dtl")
    print("=== data written into territory table in curated database=======")
    
    print("====curation process completed at " ,datetime.now())