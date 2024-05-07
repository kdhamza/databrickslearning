# Databricks notebook source
import requests
CHUNK_SIZE=4096
def get_remote_file(dataSrcUrl, destFile):
    '''Simple old skool python function to load a remote url into local hdfs '''
    destFile = "/dbfs" + destFile
    #
    with requests.get(dataSrcUrl, stream=True) as resp:
      if resp.ok:
         with open(destFile, "wb") as f:
             for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                 f.write(chunk)

# COMMAND ----------

get_remote_file('https://gitlab.com/opstar/share20/-/raw/master/policy.txt','/FileStore/tables/policy_kd.txt')

# COMMAND ----------

policy = spark.read.format('csv')\
    .option('delimiter','\t')\
    .option('header', 'true')\
    .load('/FileStore/tables/policy_kd.txt')\
    .selectExpr('int(policynum) as policynum', 'date(expiry) as expiry','location','state','region', 'cast(insuredvalue as decimal(10,2)) as insuredvalue', 'construction','businesstype','earthquake','flood')
            
policy.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table hive_kd.basic(policynum int
# MAGIC , expiry date
# MAGIC , location string
# MAGIC , state string
# MAGIC , region string
# MAGIC , insuredvalue decimal(10,2)
# MAGIC , construction string
# MAGIC , businesstype string
# MAGIC , earthquake string
# MAGIC , flood string);

# COMMAND ----------

policy.write.mode('append').saveAsTable('hive_kd.basic')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_kd.basic

# COMMAND ----------

# MAGIC %sql
# MAGIC select region
# MAGIC , sum(insuredvalue) as totalValue
# MAGIC , avg(insuredvalue) as avgValue 
# MAGIC , count(policynum) as policyCount
# MAGIC from hive_kd.basic 
# MAGIC group by region;

# COMMAND ----------

polsummary = spark.sql('select region, sum(insuredvalue) as totalValue, avg(insuredvalue) as avgValue, count(policynum) as policyCount from hive_kd.basic group by region;')

display(polsummary)
