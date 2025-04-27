# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime,dateutil
import pandas as pd


fiscalDf=spark.table("bronze.fiscalperiod")

display(fiscalDf)


# COMMAND ----------



start_date = datetime.date(2018,1,1)
end_date = start_date + dateutil.relativedelta.relativedelta(years=8,month=12,day=31)


start_date = datetime.datetime.strptime(
    f"{start_date}", "%Y-%m-%d"
)
end_date = datetime.datetime.strptime(
    f"{end_date}", "%Y-%m-%d"
)
print(start_date)
print(end_date)





# COMMAND ----------


datepddf = pd.date_range(start_date,end_date, freq='D').to_frame(name='Date')
datedf=spark.createDataFrame(datepddf)
display(datedf)

# COMMAND ----------


# COMMAND ----------


joindf = (
    datedf.join(
        fiscalDf.filter(fiscalDf.RecordId.isNotNull()),
         (datedf.Date >= fiscalDf.FiscalStartDate)
        & (datedf.Date <= fiscalDf.FiscalEndDate),
        "left",
    ))
display(joindf)




# COMMAND ----------


updatedDateTime = datetime.datetime.now()
datedimdf = joindf.select(
    "Date",
    date_format(col("Date"), "yyyyMMdd").cast("int").alias("DateId"),
    year(col("Date")).alias("Year"),
    month(col("Date")).alias("Month"),
    date_format(col("Date"), "MMM").cast("string").alias("MonthName"),
    dayofmonth(col("Date")).alias("Day"),
    date_format(col("Date"), "E").cast("string").alias("DayName"),
    quarter(col("Date")).alias("Quarter"),
    col("FiscalPeriodName").alias("FiscalPeriodName"),    
    "FiscalStartDate",
    "FiscalEndDate",
    "FiscalMonth",
    "FiscalYearStart",
    "FiscalYearEnd",
    "FiscalQuarter",
    "FiscalQuarterStart",
    "FiscalQuarterEnd",
    concat(lit("FY"),"FiscalYear").alias("FiscalYear"),
   
    xxhash64("DateId").alias("DateKey")
).withColumn("UpdatedDateTime",lit(updatedDateTime))


display(datedimdf)



# COMMAND ----------

dffinal=datedimdf
entity="dimdate"
writetoschema(dffinal,"silver",entity)
