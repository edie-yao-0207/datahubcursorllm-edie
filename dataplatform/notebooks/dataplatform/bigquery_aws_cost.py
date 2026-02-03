from datetime import datetime

from dateutil.relativedelta import relativedelta

now = datetime.utcnow()
months = []
month_count = 4
for i in range(month_count):
    month = (now - relativedelta(months=i)).strftime("%Y-%m-01")
    months.append("'" + month + "'")
partitions = f"month IN ({', '.join(months)})"
print("partitions: ", partitions)

df = spark.read.format("bigquery").option("table", "backend_test.aws_cost_v2").load()
df = df.filter(partitions)
df.write.format("delta").mode("overwrite").partitionBy("month").option(
    "mergeSchema", "true"
).option("replaceWhere", partitions).saveAsTable("bigquery.aws_cost_v2")
