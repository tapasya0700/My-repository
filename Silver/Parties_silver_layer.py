# Databricks notebook source
# MAGIC %run ../Raw/filereadandwrite

# COMMAND ----------

from pyspark.sql.functions import *
import datetime

partiesDf=spark.table("bronze.parties")
partyaddressDf=spark.table("bronze.partyaddress")
display(partiesdf)


# COMMAND ----------

display(partyaddressdf)

# COMMAND ----------


updatedDateTime=datetime.datetime.now()

dimPartyDf = partiesDf.join(
    partyaddressDf, partiesDf.PartyId == partyaddressDf.PartyNumber, "left"
    ).filter(partiesDf.RecordId.isNotNull()
    ).select(
        partiesDf.PartyId,
        trim(partiesDf.PartyName).alias("PartyName"),
        when(partiesDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(partiesDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        from_utc_timestamp(partiesDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        trim(partiesDf.PartyAddressCode).alias("PartyAddressCode"),
        from_utc_timestamp(partiesDf.EstablishedDate,'CST').alias("EstablishedDate"),
        trim(partiesDf.PartyEmailId).alias("PartyEmailId"),
        trim(partiesDf.PartyContactNumber).alias("PartyContactNumber"),
        partiesDf.RecordId.alias("PartyRecordId"),
        trim(partiesDf.TaxId).alias("TaxId"),
        trim(partyaddressDf.Address).alias("Address"),
        trim(partyaddressDf.City).alias("City"),
        trim(partyaddressDf.State).alias("State"),
        trim(partyaddressDf.Country).alias("Country"),
        trim(partyaddressDf.Region).alias("Region"),
        from_utc_timestamp(partyaddressDf.ValidFrom,'CST').alias("ValidFrom"),
        when(partyaddressDf.ValidTo.isNull(), "1900-01-01").otherwise(partyaddressDf.ValidTo).cast("timestamp").alias("ValidTo"),
        partyaddressDf.RecordId.alias("PartyAddressRecordId")    ).withColumn("UpdatedDateTime", lit(updatedDateTime)

    
    ).withColumn("PartyHashKey", xxhash64("PartyRecordId")
    )
display(dimPartyDf)

# COMMAND ----------



# COMMAND ----------

dffinal=dimPartyDf
entity="dimParty"
writetoschema(dffinal,"silver",entity)
