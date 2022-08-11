# Databricks notebook source
# DBTITLE 1,Importando algumas libs:
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime
import re
import json
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Criando uma view, e juntando as 12 tabelas:
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW experimentos_comercial.e2_base AS 
# MAGIC SELECT * 
# MAGIC FROM experimentos_comercial.e_0
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_1
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_2
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_3
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_4
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_5
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_6
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_7
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_8
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_9
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_10
# MAGIC   UNION ALL
# MAGIC SELECT *
# MAGIC FROM experimentos_comercial.e_11

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE  experimentos_comercial.e2_base

# COMMAND ----------

# DBTITLE 1,Visualizando a view:
# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM experimentos_comercial.e2_base

# COMMAND ----------

# DBTITLE 1,Criando o data frame:
df_dados = spark.sql("""
SELECT 

NU_INSCRICAO,
CAST (NU_ANO AS INT),
CAST (TP_FAIXA_ETARIA AS INT),
TP_SEXO,
CAST (TP_COR_RACA AS INT),
CAST (TP_ESCOLA AS INT),
CAST (TP_ENSINO AS INT),
CAST (IN_TREINEIRO AS INT),
CO_MUNICIPIO_ESC,
NO_MUNICIPIO_ESC,
CO_UF_ESC,
SG_UF_ESC,
CAST (TP_DEPENDENCIA_ADM_ESC AS INT),
CAST (TP_LOCALIZACAO_ESC AS INT),
CAST (TP_SIT_FUNC_ESC AS INT),
CO_MUNICIPIO_PROVA,
NO_MUNICIPIO_PROVA,
CO_UF_PROVA,
SG_UF_PROVA,
CAST (TP_PRESENCA_CN AS INT),
CAST (TP_PRESENCA_CH AS INT),
CAST (TP_PRESENCA_LC AS INT),
CAST (TP_PRESENCA_MT AS INT),
CAST (NU_NOTA_CN AS DECIMAL(10,2)),
CAST (NU_NOTA_CH AS DECIMAL(10,2)),
CAST (NU_NOTA_LC AS DECIMAL(10,2)),
CAST (NU_NOTA_MT AS DECIMAL(10,2)),
CAST (TP_LINGUA AS INT),
CAST (TP_STATUS_REDACAO AS INT),
CAST (NU_NOTA_REDACAO AS DECIMAL(10,2))

FROM experimentos_comercial.e2_base

""")

# COMMAND ----------

# DBTITLE 1,Visualizando o data frame:
df_dados.display()

# COMMAND ----------

# DBTITLE 1,Adicionando a coluna da Soma das Notas:
df_dados = df_dados.withColumn("SOMA_NOTA", df_dados['NU_NOTA_CN'] + df_dados['NU_NOTA_CH'] + df_dados['NU_NOTA_LC'] + df_dados['NU_NOTA_MT'] + df_dados['NU_NOTA_REDACAO'] )

# COMMAND ----------

# DBTITLE 1,Adicionando a coluna da Média:
df_dados = df_dados.withColumn("MÉDIA", (col("SOMA_NOTA")/5).cast(DecimalType(10,2)) )

# COMMAND ----------

# DBTITLE 1,Visualizando o data frame:
df_dados.display()

# COMMAND ----------

