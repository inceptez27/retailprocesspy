from pyspark.sql import SparkSession
from datetime import datetime


def aggrprocess(spark,prop):
    print("====aggregation process started at " ,datetime.now())
   
    #product wise aggregated data
    spark.sql("DROP DATABASE IF EXISTS retail_agg CASCADE")
    spark.sql("create database if not exists retail_agg")
    
    dfsales = spark.sql("""select s.Order_Date,sum(s.OrderQuantity) orderquantity,sum(p.ProductCost) productcost,sum(p.ProductPrice) productprice 
    from retail_curated.tblsales_dtl s inner join  retail_curated.tblproduct_dtl p on s.ProductKey = p.ProductKey group by s.Order_Date""")
      
    dfsales.write.mode("overwrite").partitionBy("Order_Date").saveAsTable("retail_agg.tbl_fact_productsales")
    
    print("=== data written into productsales table in Aggregated database=======")
   
   
    #customer wise sales data
    dfcustomer = spark.sql("""select s.Order_Date,c.Occupation,c.MaritalStatus,sum(c.CustomerKey) customercount,sum(s.OrderQuantity) orderquantity 
      from retail_curated.tblsales_dtl s inner join  retail_curated.tblcustomer_dtl c on s.CustomerKey = c.CustomerKey 
      group by s.Order_Date,c.Occupation,c.MaritalStatus""")
    
    dfcustomer.write.mode("overwrite").partitionBy("Order_Date").saveAsTable("retail_agg.tbl_fact_customersales")
    
    print("=== data written into customersales table in Aggregated database=======")
    
    
    #Region wise sales data
    dfterritory = spark.sql("""select s.Order_Date,t.Region,t.Country,t.Continent,sum(s.OrderQuantity) orderquantity 
      from retail_curated.tblsales_dtl s inner join retail_curated.tblterritory_dtl t on s.TerritoryKey  = t.SalesTerritoryKey
      group by s.Order_Date,t.Region,t.Country,t.Continent""")
    
    dfterritory.write.mode("overwrite").partitionBy("Order_Date").saveAsTable("retail_agg.tbl_fact_territorysales")
      
    print("=== data written into territorysales table in curated database=======")
    
    print("====aggregation process completed at " ,datetime.now())
   
   
   