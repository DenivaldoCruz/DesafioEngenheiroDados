
# coding: utf-8

# # Desafio Engenheiro de Dados - Semantix
# ### Canditado: Denivaldo Lopes Cruz - denivaldo.cruz@gmail.com
# ### Data: 09/07/2019

# ## HTTP requests to the NASA Kennedy Space Center WWW server
# #### Datasets: NASA_access_log_Jul95.gz e NASA_access_log_Aug95.gz
# #### Fonte: http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

# In[1]:


from pyspark.context import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_date
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum
import re


# In[2]:


if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession.builder.appName("DesafioEngenheiroDadosDenivaldo").getOrCreate()


# ### Carregando os datasets

# In[3]:


df = spark.read.text(['NASA_access_log_Jul95.gz', 'NASA_access_log_Aug95.gz'])
df.show(5, truncate=False)


# In[4]:


print(df.count())


# ### Construindo o dataset com os campos host, timestamp, codigo_retorno e total_bytes

# In[5]:


## RegEx dos campos
host_regex = r'(^\S+\.[\S+\.]+\S+)\s'
timestamp_regex = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
codigo_retorno_regex = r'\s(\d{3})\s'
total_bytes_regex = r'\s(\d+)$'

## Extracao dos campos
df_log = df.select(regexp_extract('value', host_regex, 1).alias('host'),
                    regexp_extract('value', timestamp_regex, 1).alias('timestamp'),
                    regexp_extract('value', codigo_retorno_regex, 1).cast('integer').alias('codigo_retorno'),
                    regexp_extract('value', total_bytes_regex, 1).cast('integer').alias('total_bytes'))

df_log.cache()

df_log.show(10, truncate=True)


# ### Removendo hosts vazios

# In[6]:


df_log = df_log.filter("host != ''")


# In[7]:


## Total de registros
print((df_log.count(), len(df_log.columns)))


# ## Questões

# ### 1. Número de hosts únicos: 137932

# In[8]:


df_log.agg(countDistinct("host")).show()


# ### 2. O total de erros 404: 20787

# In[9]:


df_404 = df_log.filter(df_log.codigo_retorno == "404")
df_404.count()


# ### 3. Os 5 URLs que mais causaram erro 404
#
# #### hoohoo.ncsa.uiuc.edu|  251|
# #### piweba3y.prodigy.com|  157|
# #### jbiagioni.npt.nuw...|  132|
# #### piweba1y.prodigy.com|  114|
# #### www-d4.proxy.aol.com|   91|

# In[10]:


from pyspark.sql.functions import desc
df_404.groupBy("host").count().sort(desc("count")).show()


# ### 4. Quantidade de erros 404 por dia

# In[11]:


df_404 = df_404.withColumn("dia",to_date(unix_timestamp(df_log["timestamp"], "dd/MMM/yyyy").cast("timestamp")))
df_404.show()


# In[12]:


df_404.groupBy('dia').count().orderBy('dia').show(60)


# ### 5. O total de bytes retornados

# In[13]:


df_log.select("total_bytes").groupBy().sum().show()


# In[14]:


df_log.agg(sum("total_bytes")).show()
