import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import regexp_extract

def main():
 if len(sys.argv) < 2:
     print("Usage: python problem1.py <s3_bucket_url>")
     sys.exit(1)

 bucket_url = sys.argv[1].rstrip("/")
 INPUT_PATH = f"{bucket_url}/data/application_*/*.log"
 OUTPUT_PATH = f"{bucket_url}/data/output"


 spark = (SparkSession.builder
          .appName("Problem1_LogLevelDistribution")
          .getOrCreate())

 df = spark.read.text(INPUT_PATH)

 pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"
 df_levels = df.withColumn("log_level", regexp_extract("value", pattern, 1))

 counts = (df_levels.groupBy("log_level")
           .count()
           .orderBy("count", ascending=False))
 counts.coalesce(1).write.csv(f"{OUTPUT_PATH}/problem1_counts.csv", header=True, mode="overwrite")

 sample_df = df_levels.filter(df_levels.log_level != "") \
                      .orderBy(F.rand()) \
                      .limit(10)
 sample_df.coalesce(1).write.csv(f"{OUTPUT_PATH}/problem1_sample.csv", header=True, mode="overwrite")

 total_lines = df.count()
 total_with_levels = df_levels.filter(df_levels.log_level != "").count()
 unique_levels = counts.count()

 count_list = counts.collect()
 summary_lines = "\n".join([f"  {r['log_level']:<6}: {r['count']:>10}" for r in count_list])
 summary = f"""Total log lines processed: {total_lines}
Total lines with log levels: {total_with_levels}
Unique log levels found: {unique_levels}

Log level distribution:
{summary_lines}
"""
 with open(f"{OUTPUT_PATH}/problem1_summary.txt", "w") as f:
     f.write(summary)

 spark.stop()

if __name__ == "__main__":
 main()