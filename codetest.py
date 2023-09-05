# Databricks notebook source
from urllib.request import urlopen
  
import json

url = 'https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:"nonsteroidal+anti-inflammatory+drug"'

response = urlopen(url)

data_json = json.loads(response.read())
  

print(data_json)

# COMMAND ----------

rdd = sc.parallelize([data_json])

df = spark.read.json(rdd)

# COMMAND ----------

def child_struct(nested_df):
    list_schema = [((), nested_df)]
   
    flat_columns = []

    while len(list_schema) > 0:
       
          parents, df = list_schema.pop()
          flat_cols = [  col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) for c in df.dtypes if c[1][:6] != "struct"   ]
      
          struct_cols = [  c[0]   for c in df.dtypes if c[1][:6] == "struct"   ]
      
          flat_columns.extend(flat_cols)
          
          for i in struct_cols:
                projected_df = df.select(i + ".*")
                list_schema.append((parents + (i,), projected_df))
    return nested_df.select(flat_columns)

# COMMAND ----------

from pyspark.sql.functions import *
def master_array(df):
    array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    while len(array_cols)>0:
        for c in array_cols:
            df = df.withColumn(c,explode_outer(c))
        df = child_struct(df)
        array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    return df

# COMMAND ----------

df_output = master_array(df)

# COMMAND ----------

df_output.write.format('delta').option('partitionBy','company').save(path)
