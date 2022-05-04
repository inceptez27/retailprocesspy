from pyspark.sql import SparkSession
from datetime import datetime
from retail.layers import stagingprocecss,curationprocess,aggregateprocess
import os,sys
#from pathlib import Path
def main():
    try:
        #ROOT_DIR = Path(__file__).parent.parent.parent
        #CONFIG_PATH = os.path.join(ROOT_DIR, 'app.properties')
        prop = getconfiginfo('app.properties')
        spark = SparkSession.builder \
            .config("hive.metastore.uris",prop.get("thriftserverurl")) \
            .appName("Retail-coreengine") \
            .config("spark.sql.debug.maxToStringFields", 1000) \
            .enableHiveSupport() \
            .getOrCreate()
        
        starttime = datetime.now()
        print("======process started at " , datetime.now())
        
        spark.sparkContext.setLogLevel("WARN")
      
        #===================staging load==========================
        stagingprocecss.stageprocess(spark,prop)
        
        #==================curation process======================
        curationprocess.curateprocess(spark,prop)
          
        #==================aggregation load =====================
        #aggregateprocess.aggrprocess(spark,prop)
        
        print("======process completed at " , datetime.now())
        endtime = datetime.now()
        
        timetaken = endtime - starttime
        
        print("Time taken to complete the process:",timetaken.seconds ,"seconds")
        
    except Exception as e:
        exc_type, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    else:
        print("No Error")

    finally:
        print("Finally block always executed")

def getconfiginfo(configfile):
    
    separator = "="
    props = {}
    with open(configfile) as f:

        for line in f:
            if separator in line:
                name, value = line.split(separator, 1)
                props[name.strip()] = value.strip()
        
    
    return props  


main()
