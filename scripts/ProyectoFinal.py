#!/usr/bin/env python
# coding: utf-8

# # Proyeco Final - Ciencia De Datos En Python - Universidad Galileo
# ##### Francisco Duarte - 17001004

# ## Scope
# El proyecto es sobre las importaciones de vehículos a Guatemala durante el 2022, a la fecha de realziación de este proyecto solamente se cuenta con los datos para los meses de enero y febrero. Estos datos fueron obtenidos desde el portal de la Superintendencia de Administración Tributaria (SAT), son dos archivos extensión .txt, los datos se encuentra sperados por el símbolo "|"; pero se convertieron a csv para una mejor compatibilidad con workbench. Se realizó otro archivo que contiene las aduanas, estas fueron copiadas desde una tabla del portal de la SAT.
# 
# Para objetivos del proyectos uno de los archivos se importará a una base de datos en el servicio RDS de AWS, y los otros dos estarán alojadas en un contedor S3 de AWS. Se utilizará librerías como pandas para el proceso de de ETL, se construirá un datawarehouse para almacenar los datos ya procesados y se alojarán en un Redshift de AWS. RDS tendrá los datos para enero 2022 y S3 febrero 2022 y las aduanas

# ## Exploración

# ##### import necesarios

# In[1]:


get_ipython().run_line_magic('load_ext', 'sql')


# In[2]:


import pandas as pd
import os
import configparser
import sys
import boto3
from sqlalchemy import create_engine


# ### Conexiones

# In[3]:


#Cargando el archivo de configuracion
config = configparser.ConfigParser()
config.read_file(open("config.cfg"))


# In[4]:


#Conexión a la base de datos en RDS
mysql_connection = "mysql+pymysql://{}:{}@{}/{}".format(
config.get("RDS", "DB_USER"),
config.get("RDS", "DB_PASSWORD"),
config.get("RDS", "DB_HOST"),
config.get("RDS", "DB_NAME"))


# In[5]:


get_ipython().run_line_magic('sql', '$mysql_connection')


# In[47]:


#Conexión al datawarehouse en Redshift
redshift_connection = "postgresql://{}:{}@{}:{}/{}".format(
config.get("Redshift", "DB_USER"),
config.get("Redshift", "DB_PASSWORD"),
config.get("Redshift", "DB_HOST"),
config.get("Redshift", "DB_PORT"),
config.get("Redshift", "DB_NAME"))


# In[48]:


get_ipython().run_line_magic('sql', '$redshift_connection')


# In[6]:


#Conexión a S3
s3_connection = boto3.resource(
service_name = "s3",
region_name = config.get("S3", "REGION"),
aws_access_key_id = config.get("S3", "ACCESS"),
aws_secret_access_key = config.get("S3", "SECRET"))


# ### Obteniendo los datos desde RDS y S3

# #### RDS

# In[7]:


rds_importacion_select = "SELECT * FROM importacion;"
dfImportacionRDS = pd.read_sql(rds_importacion_select, mysql_connection)
dfImportacionRDS.head()


# #### S3

# In[9]:


dfImportacionS3 = pd.DataFrame()
dfAduanasS3 = pd.DataFrame()

for remoteFile in s3_connection.Bucket(config.get("S3", "BUCKET_NAME")).objects.all():
    file = s3_connection.Bucket(config.get("S3", "BUCKET_NAME")).Object(remoteFile.key).get()
    if("importacion" in remoteFile.key):
       data = pd.read_csv(file["Body"], delimiter = "|")
       dfImportacionS3 = dfImportacionS3.append(data)
    else:
       data = pd.read_csv(file["Body"], delimiter = ";")
       dfAduanasS3 = dfAduanasS3.append(data)


# In[11]:


dfImportacionS3.head()


# In[12]:


dfAduanasS3.head()


# #### Explorando los datos obtenidos

# ##### Conteo de los dataframe

# In[13]:


dfImportacionRDS.count()


# In[14]:


dfImportacionS3.count()


# In[15]:


dfAduanasS3.count()


# ##### Valors distintos columna País de proviniencia

# In[16]:


dfImportacionRDS["Pais de Proveniencia"].unique()


# In[17]:


dfImportacionS3["Pais de Proveniencia"].unique()


# ##### Valores distintos Aduana ingreso

# In[18]:


dfImportacionRDS["Aduana de Ingreso"].unique()


# In[19]:


dfImportacionS3["Aduana de Ingreso"].unique()


# In[20]:


dfAduanasS3["Aduanas"].unique()


# ##### Valores distintos en la columna Marca

# In[21]:


dfImportacionRDS["Marca"].unique()


# In[22]:


dfImportacionS3["Marca"].unique()


# ##### Valores distintos en la columna Linea

# In[23]:


dfImportacionRDS["Linea"].unique()


# In[24]:


dfImportacionS3["Linea"].unique()


# ##### Valores distintos columna Tipo importador

# In[25]:


dfImportacionRDS["Tipo de Importador"].unique()


# In[26]:


dfImportacionS3["Tipo de Importador"].unique()


# ##### Relación entre columna Linea Marca

# In[27]:


dfImportacionRDS.groupby(["Linea"])["Marca"].count()


# ##### Columna Distintivo

# In[28]:


dfImportacionRDS["Distintivo"].unique()


# In[29]:


dfImportacionS3["Distintivo"].unique()


# #### Análisis exploración

# Como resultado de la exploración de los datos se pudo encontrar que para la imortación hecha desde S3, hay una columna que se puede descartar ya que no posee ningún valor. También se determinó que diferentes marcas pueden tener las mismas líneas, el distintivo en algunas ocasiones viene con un null, así como en la aduana de ingreso en ocasiones contiene valores incorrectos, por ejemplo: "vehiculos", en relación a las aduanas los datos obtenidos del archivo Aduandas alojado en S3, el nombre de la aduanda es del tipo "Aduana Puerto Barrarios" mientras que en RDS y el otro archivo en S3 tiene las aduanas como "Puerto Barrios", otra diferencia es columna "Es almacenadora" ya que las importaciones de vehículos no trae esta información y es una columna que para este caso no aporta mayor información por lo que puede ser eliminada.
# 

# ## Modelo De Datos

# Como resultado del análisis anterior se creará el siguiente modelo:
# - dimensión Paises
# - dimensión Aduanas
# - dimensión Partida Arancelaria
# - dimensión Fecha
# - dimensión Tipo Importador
# - dimensión Vehiculo
# - tabla de hechos Importaciones

# ## Procesamiento

# ##### Uniendo los dataframe de importaciones

# In[10]:


dfImportaciones = pd.DataFrame()

dfImportaciones = dfImportaciones.append(dfImportacionRDS)

#Cambio de nombre columna Fecha de la Poliza en dfImportacionS3 para match con dfImportacionRDS
dfImportacionS3 = dfImportacionS3.rename(columns = {" Fecha de la Poliza" : "Fecha de la Poliza" })
dfImportaciones = dfImportaciones.append(dfImportacionS3.iloc[:,0:17])
dfImportaciones.count()


# In[31]:


dfImportaciones.head()


# ##### Creando dataframe para las aduanas

# In[11]:


dfAduanas = pd.DataFrame()

dfAduanas = pd.concat([dfAduanas, dfAduanasS3["Aduanas"].str.upper()], axis = 1)
dfAduanas = dfAduanas.rename(columns = {"Aduanas": "Aduana"})
dfAduanas["Nombre Corto"] = [aduana.split("ADUANA")[1].strip() if len(aduana.split("ADUANA")) == 2 else aduana for aduana in dfAduanas["Aduana"]]
dfAduanas.head()


# In[12]:


dfTempAduanas = pd.DataFrame({"Aduanas" :dfImportaciones["Aduana de Ingreso"].unique()})

for item in dfTempAduanas["Aduanas"]:
    if(item  not in dfAduanas["Nombre Corto"].values and item not in dfAduanas["Aduana"].values):
        dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
        
dfAduanas


# ### Creando las Dimensiones

# ##### Dimensión Aduanas

# In[13]:


dimAduana = pd.DataFrame(columns= ["aduana_sk"])

dimAduana = dimAduana.append(dfAduanas)
dimAduana = dimAduana.rename(columns = {"Aduana":"aduana", "Nombre Corto":"nombre_corto"})
dimAduana["aduana_sk"] = range(1, len(dimAduana) + 1)
dimAduana.head()


# ##### Dimensión Fecha

# In[14]:


dimFecha = pd.DataFrame()

dimFecha = pd.concat([dimFecha, dfImportaciones["Fecha de la Poliza"]], axis = 1)
dimFecha = dimFecha.rename(columns = {"Fecha de la Poliza": "fecha"})
dimFecha = dimFecha.drop_duplicates(subset = "fecha")
dimFecha["fecha"] = pd.to_datetime(dimFecha["fecha"], format = "%d/%m/%Y")
dimFecha.head()


# In[15]:


dimFecha["anio"] = pd.DatetimeIndex(dimFecha["fecha"]).year
dimFecha["mes"] = pd.DatetimeIndex(dimFecha["fecha"]).month
dimFecha["trimestre"] = pd.DatetimeIndex(dimFecha["fecha"]).quarter
dimFecha["dia"] = pd.DatetimeIndex(dimFecha["fecha"]).day
dimFecha["semana"] = pd.DatetimeIndex(dimFecha["fecha"]).week
dimFecha["dia_semana"] = pd.DatetimeIndex(dimFecha["fecha"]).dayofweek
dimFecha.head()


# ##### Dimensión Tipo Importador

# In[16]:


dimTipoImportador = pd.DataFrame(columns = ["tipo_importador_sk"])

dimTipoImportador = pd.concat([dimTipoImportador, dfImportaciones["Tipo de Importador"].drop_duplicates()])
dimTipoImportador = dimTipoImportador.rename(columns = {0:"tipo_importador"})
dimTipoImportador["tipo_importador_sk"] = range(1, len(dimTipoImportador)+1)
dimTipoImportador.head()


# ##### Dimensión Partida arancelaria

# In[17]:


dimPartidaArancelaria = pd.DataFrame(columns = ["partida_arancelaria_sk"])

dimPartidaArancelaria = pd.concat([dimPartidaArancelaria, dfImportaciones["Partida Arancelaria"]])
dimPartidaArancelaria = dimPartidaArancelaria.rename(columns = {0: "partida_arancelaria"})
dimPartidaArancelaria["partida_arancelaria"] = pd.to_numeric(dimPartidaArancelaria["partida_arancelaria"], downcast = "integer")
dimPartidaArancelaria = dimPartidaArancelaria.drop_duplicates()
dimPartidaArancelaria["partida_arancelaria_sk"] = range(1, len(dimPartidaArancelaria)+1)
dimPartidaArancelaria.head()


# ##### Dimensión Países

# In[18]:


dimPais = pd.DataFrame(columns = ["pais_sk"])

dimPais = pd.concat([dimPais, dfImportaciones["Pais de Proveniencia"].drop_duplicates()])
dimPais = dimPais.rename(columns = {0: "pais"})
dimPais["pais_sk"] = range(1, len(dimPais) + 1)
dimPais.head()


# ##### Dimensión Vehiculos

# In[19]:


dimVehiculo = pd.DataFrame(columns = ["vehiculo_sk"])

dimVehiculo = pd.concat([dimVehiculo, dfImportaciones.loc[:,["Modelo del Vehiculo", "Marca", "Linea", "Centimetros Cubicos", "Distintivo", 
                                                      "Tipo de Vehiculo", "Tipo Combustible", "Asientos", "Puertas", "Tonelaje"]]])
dimVehiculo = dimVehiculo.rename(columns = {"Modelo del Vehiculo": "modelo", "Marca":"marca", "Linea":"linea", "Centimetros Cubicos" : "centimetros_cubicos",
                                         "Distintivo":"distintivo", "Tipo de Vehiculo": "tipo", "Tipo Combustible": "combustible", "Asientos": "asientos",
                                         "Puertas": "puertas", "Tonelaje": "tonelaje"})
dimVehiculo = dimVehiculo.drop_duplicates()
dimVehiculo["vehiculo_sk"] = range(1, len(dimVehiculo) +1)
dimVehiculo.head()


# ### Creando la Tabla de Hechos

# In[20]:


factImportaciones = pd.DataFrame()

factImportaciones = factImportaciones.append(dfImportaciones)
factImportaciones["Fecha de la Poliza"] = pd.to_datetime(factImportaciones["Fecha de la Poliza"], format = "%d/%m/%Y")
factImportaciones = factImportaciones.rename(columns = {"Valor CIF": "valor_cif", "Impuesto":"impuesto"})

#merge fact y dimensiones
factImportaciones = factImportaciones.merge(dimPais, left_on="Pais de Proveniencia", right_on="pais").drop(columns = ["Pais de Proveniencia", "pais"])
factImportaciones = factImportaciones.merge(dimAduana[["aduana_sk", "nombre_corto"]], left_on="Aduana de Ingreso", right_on="nombre_corto").drop(columns = ["Aduana de Ingreso", "nombre_corto"])
factImportaciones = factImportaciones.merge(dimFecha[["fecha"]], left_on="Fecha de la Poliza", right_on="fecha").drop(columns = ["Fecha de la Poliza"])
factImportaciones = factImportaciones.merge(dimTipoImportador, left_on="Tipo de Importador", right_on="tipo_importador").drop(columns = ["Tipo de Importador", "tipo_importador"])
factImportaciones = factImportaciones.merge(dimVehiculo, left_on=["Modelo del Vehiculo", "Marca", "Linea", "Centimetros Cubicos", "Distintivo", 
                                                      "Tipo de Vehiculo", "Tipo Combustible", "Asientos", "Puertas", "Tonelaje"], right_on=["modelo", "marca", "linea", "centimetros_cubicos", "distintivo", 
                                                      "tipo", "combustible", "asientos", "puertas", "tonelaje"]).drop(columns = ["Modelo del Vehiculo", "Marca", "Linea", "Centimetros Cubicos", "Distintivo", 
                                                      "Tipo de Vehiculo", "Tipo Combustible", "Asientos", "Puertas", "Tonelaje","modelo", "marca", "linea", "centimetros_cubicos", "distintivo", 
                                                      "tipo", "combustible", "asientos", "puertas", "tonelaje"])
factImportaciones = factImportaciones.merge(dimPartidaArancelaria, left_on="Partida Arancelaria", right_on="partida_arancelaria").drop(columns = ["Partida Arancelaria", "partida_arancelaria"])

factImportaciones = factImportaciones[["fecha", "pais_sk", "aduana_sk", "partida_arancelaria_sk", "tipo_importador_sk", "vehiculo_sk","valor_cif", "impuesto"]]
factImportaciones


# ### Cargando a Redshift

# In[52]:


connection_engine = create_engine(redshift_connection)


# In[55]:


#Insert de las dimensiones y tabla de hechos
dimAduana.to_sql("dim_aduana", connection_engine, index = False, if_exists = "append")
dimFecha.to_sql("dim_fecha", connection_engine, index = False, if_exists = "append", method="multi")
dimTipoImportador.to_sql("dim_tipo_importador", connection_engine, index = False, if_exists = "append")
dimPartidaArancelaria.to_sql("dim_partida_arancelaria", connection_engine, index = False, if_exists = "append", method="multi")
dimPais.to_sql("dim_pais", connection_engine, index = False, if_exists = "append", method="multi")
dimVehiculo.to_sql("dim_vehiculo", connection_engine, index = False, if_exists = "append", method="multi")
factImportaciones.to_sql("fact_importaciones", connection_engine, index = False, if_exists = "append", method="multi")


# ## Analitica

# ##### ¿Cuál fue el monto recaudado en impuesto para enero y febrero de 2022?

# In[56]:


sum(factImportaciones["impuesto"])


# El impuesto recaudado para enero y febrero 2022 es de Q. 8,297,207,141.79

# ##### ¿Cuánto es el impuesto recaudado por día?

# In[152]:


factImportaciones.groupby("fecha")["impuesto"].sum()


# ##### ¿Cuál el valor promedio de la importaciones?

# In[154]:


factImportaciones["valor_cif"].mean()


# El valor CIF promedio de las importaciones es de Q. 904,913.05

# ##### ¿Cuál el monto de la importación de mayor valor?

# In[155]:


factImportaciones["valor_cif"].max()


# La importación de mayor valor es de Q. 5,199,182.49

# ##### ¿Cuál es el monto de la importación de menor valor?

# In[21]:


factImportaciones["valor_cif"].min()


# La importación de menor valor es de Q. 1,964.84

# ##### ¿Cuánto es el país de donde más importa?

# In[28]:


dfTemp = pd.DataFrame()
dfTemp = pd.concat([dfTemp,factImportaciones["pais_sk"]], axis =1)
dfTemp = dfTemp.merge(dimPais, left_on="pais_sk", right_on="pais_sk")
dfTemp.groupby("pais")["pais_sk"].count().sort_values(ascending= False)


# El país de donde más se importan vehículos es China

# ##### ¿Cuántos vehiculos ingresan por aduana?

# In[33]:


dfTemp = pd.DataFrame()
dfTemp = pd.concat([dfTemp,factImportaciones["aduana_sk"]], axis =1)
dfTemp = dfTemp.merge(dimAduana, left_on="aduana_sk", right_on="aduana_sk")
dfTemp.groupby("aduana")["aduana_sk"].count().sort_values(ascending= False)


# La aduana donde ingresan más vehículos es la Aduana de Puerto Quetzal

# In[ ]:




