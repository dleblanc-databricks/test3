# Databricks notebook source
display(dbutils.fs.ls('/'))

# COMMAND ----------

import os
print(os.listdir('/dbfs/databricks-results'))

# COMMAND ----------

df = spark.read.csv("/FileStore/uszips.csv",
                    header="true", 
                    inferSchema="true")

display(df)

# COMMAND ----------

print(spark)

# COMMAND ----------

df.createOrReplaceTempView('temp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `state_id` AS `state`,COUNT(`zip`) AS `nzips`
# MAGIC FROM temp
# MAGIC WHERE `state_id` NOT IN ('AS','GU','MP','PR','VI') 
# MAGIC GROUP BY `state` 
# MAGIC ORDER BY `nzips` 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `state_id` AS `state`,SUM(`population`) AS `population`
# MAGIC FROM temp
# MAGIC WHERE `state_id` NOT IN ('AS','GU','MP','PR','VI')
# MAGIC GROUP BY `state`

# COMMAND ----------


