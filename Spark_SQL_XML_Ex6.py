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

get_remote_file('https://gitlab.com/opstar/share20/-/raw/master/worldfact.xml', '/mydata/worldfact_kd.xml' )

# COMMAND ----------

raw = spark.read.format('xml')\
 .option('rowTag', 'country')\
 .load('/mydata/worldfact_kd.xml')

raw.printSchema()

# COMMAND ----------

countries = raw.select('_name', '_population')\
    .withColumnsRenamed({
        '_name': 'countryname', 
        '_population': 'countrypop'
        })
display(countries)

# COMMAND ----------

from pyspark.sql.functions import explode, trim, regexp_replace

city_df = raw.select('_name', '_population', explode('city').alias('city'))
city_df = city_df.select('_name', '_population', explode('city.name'), 'city.population._VALUE')\
    .withColumnsRenamed({
        '_name': 'countryname', 
        '_population': 'countrypop', 
        'col': 'cityname', 
        '_VALUE': 'citypop'
        })\
    .withColumn('cityname',trim(regexp_replace('cityname','[\n]+','')))\
    .withColumn('citypop',trim(regexp_replace('citypop','[\n]+','')))\
    .withColumn('countrypop',trim(regexp_replace('countrypop','[\n]+','')))\
    .selectExpr('countryname','int(countrypop) as countrypop','cityname', 'int(citypop) as citypop')
display(city_df)

# COMMAND ----------

from pyspark.sql.functions import col

city_pct = city_df.withColumn('citypct',(city_df.citypop / city_df.countrypop *100))\
    .orderBy(col('citypct'), acsending= False)
display(city_pct)
