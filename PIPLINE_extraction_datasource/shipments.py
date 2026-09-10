from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.functions import when, col

### admissions data ingestions
# Define S3 path
def path_s3_bucket():
    data_source = "shipments.csv"
    bucket = "source-system-mysql-sqlservers"
    prefix = f"{data_source}"
    buck = "sephoraa"
    s3_path = f"s3://{bucket}/{buck}/{prefix}"
    return s3_path,data_source
s3_path,data_source = path_s3_bucket()
print(s3_path)
# s3_path = "s3_path"

class data_ingestion:
    def letest_file_update_only(self,s3_path):
        self.s3_path = s3_path
        # print(s3_path)
        # Use dbutils to list files and find the latest
        files = dbutils.fs.ls(s3_path)
        latest_file = sorted(files, key=lambda x: x.modificationTime, reverse=True)[0]
        return latest_file.path
s1 = data_ingestion()
base_path = s1.letest_file_update_only(s3_path)
print(base_path)
from datetime import datetime
from pyspark.sql.functions import lit
### Read CSV
class read_records:
    def reading_data(self):
        df = (
            spark.read.format("csv")
             .option("header",True)
             .option("inferSchema",True)
            .load(base_path)
            )
        df = df.withColumn("load_ts",
                          F.current_date())
        # self.df = df
        # return df
        return df

asd = read_records()
asd.reading_data()
df = asd.reading_data()
# display(df)

from pyspark.sql.functions import year, month, dayofmonth
# Add year, month, and day columns derived from load_ts
df = df.withColumn("year", year("load_ts")) \
                   .withColumn("month", month("load_ts")) \
                   .withColumn("day", dayofmonth("load_ts"))
#display(df)

#Save data
## bronze write to s3
from pyspark.sql.utils import AnalysisException
import traceback

class FinalLoad:
    def loading_final_bronze_order(self, df, data_source):
        try:
            # Attempt to write DataFrame to csv
            df.write.format("csv") \
                .option("header", "true")\
                .mode("overwrite") \
                .partitionBy("year","month","day") \
                .save(f"s3://sephora-target/raw/{data_source}/")
             # .option("overwriteSchema", "true") \
            print("✅ Data successfully written to RAW layer.")
        
        except AnalysisException as ae:
            print("AnalysisException occurred while writing data:")
            print(ae)
        
        except Exception as e:
            print("Unexpected error occurred while writing data:")
            print(str(e))
            traceback.print_exc()   # optional: prints full stack trace for debugging

# Usage
TAD = FinalLoad()
TAD.loading_final_bronze_order(df, data_source="shipments")