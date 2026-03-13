# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1e76996d-e401-4e80-a093-5335b6790edc",
# META       "default_lakehouse_name": "LH_Wind_Power_Bronze",
# META       "default_lakehouse_workspace_id": "96862bf5-946f-467d-bb52-1836f5a39ed1",
# META       "known_lakehouses": [
# META         {
# META           "id": "1e76996d-e401-4e80-a093-5335b6790edc"
# META         },
# META         {
# META           "id": "ad21c0d8-914f-46f1-87ff-7b7ac63fcfa0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#test

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to the wind_power table in the Bronze Lakehouse
bronze_table_path = "abfss://WindPowerAnalytics_ManalSoulane@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Bronze.Lakehouse/Tables/dbo/wind_power"
# Load the wind_power table into a DataFrame
df = spark.read.format("delta").load(bronze_table_path)
# Display the Bronze data
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
col, round,
dayofmonth, month, quarter, year,
regexp_replace, substring, when
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and enrich data
df_transformed = (
df
.withColumn("wind_speed", round(col("wind_speed"), 2))
.withColumn("energy_produced", round(col("energy_produced"), 2))
.withColumn("day", dayofmonth(col("date")))
.withColumn("month", month(col("date")))
.withColumn("quarter", quarter(col("date")))
.withColumn("year", year(col("date")))
.withColumn("time", regexp_replace(col("time"), "-", ":"))
.withColumn("hour_of_day", substring(col("time"), 1, 2).cast("int"))
.withColumn("minute_of_hour", substring(col("time"), 4, 2).cast("int"))
.withColumn("second_of_minute", substring(col("time"), 7,
2).cast("int"))
.withColumn(
"time_period",
when((col("hour_of_day") >= 5) & (col("hour_of_day") < 12),
"Morning")
.when((col("hour_of_day") >= 12) & (col("hour_of_day") < 17),
"Afternoon")
.when((col("hour_of_day") >= 17) & (col("hour_of_day") < 21),
"Evening")
.otherwise("Night")
)
)
# Display the transformed data
display(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to the wind_power table in the Silver Lakehouse
silver_table_path = "abfss://WindPowerAnalytics_ManalSoulane@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Silver.Lakehouse/Tables/dbo/wind_power"
# Save the transformed table to the Silver Lakehouse
df_transformed.write.format("delta").mode("overwrite").save(silver_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display the Bronze data
display(df)
# Display the transformed data
display(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
