# Proyeco Final - Ciencia De Datos En Python - Universidad Galileo
##### Francisco Duarte - 17001004

## Scope
El proyecto es sobre las importaciones de vehículos a Guatemala durante el 2022, a la fecha de realziación de este proyecto solamente se cuenta con los datos para los meses de enero y febrero. Estos datos fueron obtenidos desde el portal de la Superintendencia de Administración Tributaria (SAT), son dos archivos extensión .txt, los datos se encuentra sperados por el símbolo "|"; pero se convertieron a csv para una mejor compatibilidad con workbench. Se realizó otro archivo que contiene las aduanas, estas fueron copiadas desde una tabla del portal de la SAT.

Para objetivos del proyectos uno de los archivos se importará a una base de datos en el servicio RDS de AWS, y los otros dos estarán alojadas en un contedor S3 de AWS. Se utilizará librerías como pandas para el proceso de de ETL, se construirá un datawarehouse para almacenar los datos ya procesados y se alojarán en un Redshift de AWS. RDS tendrá los datos para enero 2022 y S3 febrero 2022 y las aduanas

## Exploración

##### import necesarios


```python
%load_ext sql
```


```python
import pandas as pd
import os
import configparser
import sys
import boto3
from sqlalchemy import create_engine
```

### Conexiones


```python
#Cargando el archivo de configuracion
config = configparser.ConfigParser()
config.read_file(open("config.cfg"))
```


```python
#Conexión a la base de datos en RDS
mysql_connection = "mysql+pymysql://{}:{}@{}/{}".format(
config.get("RDS", "DB_USER"),
config.get("RDS", "DB_PASSWORD"),
config.get("RDS", "DB_HOST"),
config.get("RDS", "DB_NAME"))
```


```python
%sql $mysql_connection
```




    'Connected: admin@importacion-vehiculos'




```python
#Conexión al datawarehouse en Redshift
redshift_connection = "postgresql://{}:{}@{}:{}/{}".format(
config.get("Redshift", "DB_USER"),
config.get("Redshift", "DB_PASSWORD"),
config.get("Redshift", "DB_HOST"),
config.get("Redshift", "DB_PORT"),
config.get("Redshift", "DB_NAME"))
```


```python
%sql $redshift_connection
```




    'Connected: awsuser@dev'




```python
#Conexión a S3
s3_connection = boto3.resource(
service_name = "s3",
region_name = config.get("S3", "REGION"),
aws_access_key_id = config.get("S3", "ACCESS"),
aws_secret_access_key = config.get("S3", "SECRET"))
```

### Obteniendo los datos desde RDS y S3

#### RDS


```python
rds_importacion_select = "SELECT * FROM importacion;"
dfImportacionRDS = pd.read_sql(rds_importacion_select, mysql_connection)
dfImportacionRDS.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Pais de Proveniencia</th>
      <th>Aduana de Ingreso</th>
      <th>Fecha de la Poliza</th>
      <th>Partida Arancelaria</th>
      <th>Modelo del Vehiculo</th>
      <th>Marca</th>
      <th>Linea</th>
      <th>Centimetros Cubicos</th>
      <th>Distintivo</th>
      <th>Tipo de Vehiculo</th>
      <th>Tipo de Importador</th>
      <th>Tipo Combustible</th>
      <th>Asientos</th>
      <th>Puertas</th>
      <th>Tonelaje</th>
      <th>Valor CIF</th>
      <th>Impuesto</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
  </tbody>
</table>
</div>



#### S3


```python
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
```

    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/974039967.py:11: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanasS3 = dfAduanasS3.append(data)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/974039967.py:8: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfImportacionS3 = dfImportacionS3.append(data)



```python
dfImportacionS3.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Pais de Proveniencia</th>
      <th>Aduana de Ingreso</th>
      <th>Fecha de la Poliza</th>
      <th>Partida Arancelaria</th>
      <th>Modelo del Vehiculo</th>
      <th>Marca</th>
      <th>Linea</th>
      <th>Centimetros Cubicos</th>
      <th>Distintivo</th>
      <th>Tipo de Vehiculo</th>
      <th>Tipo de Importador</th>
      <th>Tipo Combustible</th>
      <th>Asientos</th>
      <th>Puertas</th>
      <th>Tonelaje</th>
      <th>Valor CIF</th>
      <th>Impuesto</th>
      <th>Unnamed: 17</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>INDIA</td>
      <td>PUERTO QUETZAL</td>
      <td>08/02/2022</td>
      <td>8711209000</td>
      <td>2023</td>
      <td>TVS</td>
      <td>APACHE RTR 160</td>
      <td>159.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>1389415.13</td>
      <td>166729.82</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>INDIA</td>
      <td>PUERTO QUETZAL</td>
      <td>08/02/2022</td>
      <td>8711209000</td>
      <td>2023</td>
      <td>TVS</td>
      <td>APACHE RTR 160</td>
      <td>159.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>1389415.13</td>
      <td>166729.82</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>INDIA</td>
      <td>PUERTO QUETZAL</td>
      <td>08/02/2022</td>
      <td>8711209000</td>
      <td>2023</td>
      <td>TVS</td>
      <td>APACHE RTR 160</td>
      <td>159.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>1389415.13</td>
      <td>166729.82</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>INDIA</td>
      <td>PUERTO QUETZAL</td>
      <td>08/02/2022</td>
      <td>8711209000</td>
      <td>2023</td>
      <td>TVS</td>
      <td>APACHE RTR 160</td>
      <td>159.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>1389415.13</td>
      <td>166729.82</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>INDIA</td>
      <td>PUERTO QUETZAL</td>
      <td>08/02/2022</td>
      <td>8711209000</td>
      <td>2023</td>
      <td>TVS</td>
      <td>APACHE RTR 160</td>
      <td>159.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>1389415.13</td>
      <td>166729.82</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
dfAduanasS3.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Aduanas</th>
      <th>Es Almacenadora</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Aduana Tecún Umán 1</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Aduana Integrada Agua Caliente</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Aduana Integrada Corinto</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Aduana Integrada El Florido</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Aduana Puerto Quetzal</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



#### Explorando los datos obtenidos

##### Conteo de los dataframe


```python
dfImportacionRDS.count()
```




    Pais de Proveniencia    38988
    Aduana de Ingreso       38988
    Fecha de la Poliza      38988
    Partida Arancelaria     38988
    Modelo del Vehiculo     38988
    Marca                   38988
    Linea                   38988
    Centimetros Cubicos     38983
    Distintivo              37565
    Tipo de Vehiculo        38988
    Tipo de Importador      38988
    Tipo Combustible        38988
    Asientos                38988
    Puertas                 38988
    Tonelaje                38988
    Valor CIF               38988
    Impuesto                38988
    dtype: int64




```python
dfImportacionS3.count()
```




    Pais de Proveniencia    37462
    Aduana de Ingreso       37462
     Fecha de la Poliza     37462
    Partida Arancelaria     37462
    Modelo del Vehiculo     37462
    Marca                   37462
    Linea                   37462
    Centimetros Cubicos     37453
    Distintivo              37308
    Tipo de Vehiculo        37462
    Tipo de Importador      37462
    Tipo Combustible        37462
    Asientos                37462
    Puertas                 37462
    Tonelaje                37462
    Valor CIF               37462
    Impuesto                37462
    Unnamed: 17                 0
    dtype: int64




```python
dfAduanasS3.count()
```




    Aduanas            31
    Es Almacenadora     8
    dtype: int64



##### Valors distintos columna País de proviniencia


```python
dfImportacionRDS["Pais de Proveniencia"].unique()
```




    array(['CHINA', 'INDIA', 'CANADA', 'ARGENTINA', 'COREA DEL SUR',
           'ESTADOS UNIDOS', 'JAPON', 'REINO UNIDO', 'MEXICO', 'TAILANDIA',
           'ITALIA', 'BRASIL', 'ALEMANIA REP. FED.', 'INDONESIA', 'SUECIA',
           'FRANCIA', 'BELICE', 'TAIWAN', 'HUNGRIA', 'BELGICA', 'AUSTRIA',
           'SUDAFRICA', 'COMOROS', 'TURQUIA', 'SLOVAKIA', 'AUSTRALIA',
           'IRLANDA', 'ESPANA', 'COLOMBIA'], dtype=object)




```python
dfImportacionS3["Pais de Proveniencia"].unique()
```




    array(['INDIA', 'ESTADOS UNIDOS', 'CANADA', 'JAPON', 'CHINA',
           'REINO UNIDO', 'FRANCIA', 'ALEMANIA REP. FED.', 'ITALIA', 'MEXICO',
           'BELICE', 'COREA DEL SUR', 'BRASIL', 'INDONESIA', 'TAILANDIA',
           'U.R.S.S.', 'SUECIA', 'TAIWAN', 'AUSTRIA', 'AUSTRALIA', 'SUIZA',
           'ESPANA', 'KENYA', 'TURQUIA', 'SLOVAKIA', 'BELGICA', 'ARGENTINA',
           'HUNGRIA', 'PANAMA'], dtype=object)



##### Valores distintos Aduana ingreso


```python
dfImportacionRDS["Aduana de Ingreso"].unique()
```




    array(['PUERTO QUETZAL', 'EL CARMEN', 'G8, CENTRALSA', 'G1, INTEGRADA',
           'SANTO TOMAS DE CASTILLA', 'PUERTO BARRIOS', 'TECUN UMAN',
           'G4, ALSERSA', 'MELCHOR DE MENCOS', 'EL CEIBO', 'LA MESILLA',
           'VALLE NUEVO', 'PEDRO DE ALVARADO', 'G5, CEALSA', 'VEHICULOS',
           'ADUANA INTEGRADA AGUA CALIENTE', 'G7, ALCORSA', 'G3, ALPASA',
           'SAN CRISTOBAL', 'ADUANA INTEGRADA CORINTO', 'LA ERMITA'],
          dtype=object)




```python
dfImportacionS3["Aduana de Ingreso"].unique()
```




    array(['PUERTO QUETZAL', 'TECUN UMAN', 'G1, INTEGRADA', 'G8, CENTRALSA',
           'EL CARMEN', 'PUERTO BARRIOS', 'SANTO TOMAS DE CASTILLA',
           'MELCHOR DE MENCOS', 'VEHICULOS', 'LA MESILLA',
           'PEDRO DE ALVARADO', 'SAN CRISTOBAL', 'EL CEIBO',
           'ADUANA INTEGRADA AGUA CALIENTE', 'VALLE NUEVO', 'G3, ALPASA',
           'G7, ALCORSA', 'G4, ALSERSA', 'ADUANA INTEGRADA CORINTO'],
          dtype=object)




```python
dfAduanasS3["Aduanas"].unique()
```




    array(['Aduana Tecún Umán 1', 'Aduana Integrada Agua Caliente',
           'Aduana Integrada Corinto', 'Aduana Integrada El Florido',
           'Aduana Puerto Quetzal', 'Aduana Santo Tomas De Castilla',
           'Aduana Express Aereo', 'Aduana Central', 'Aduana Tecun Uman',
           'Aduana Puerto Barrios', 'Aduana Pedro De Alvarado',
           'Aduana San Cristobal',
           'Aduana Almacenadora Integrada (almacenadora)',
           'Aduana Agua Caliente', 'Aduana El Florido',
           'Aduana Alpasa (almacenadora)', 'Aduana Centralsa (almacenadora)',
           'Aduana La Ermita', 'Aduana El Carmen', 'Aduana Valle Nuevo',
           'Aduana Alcorsa (almacenadora)', 'Aduana Alminter (almacenadora)',
           'Aduana Alsersa (almacenadora)', 'Aduana Cealsa (almacenadora)',
           'Aduana Almaguate (almacenadora)', 'Aduana Central De Aviacion',
           'Aduana Melchor De Mencos', 'Aduana Fardos Postales', 'El Ceibo',
           'Aduana La Mesilla', 'Tikal'], dtype=object)



##### Valores distintos en la columna Marca


```python
dfImportacionRDS["Marca"].unique()
```




    array(['YAMAHA', 'HONDA', 'TOYOTA', 'KIA', 'CHEVROLET', 'HYUNDAI', 'JEEP',
           'SUZUKI', 'FORD', 'NISSAN', 'MOVESA', 'MAZDA', 'KAWASAKI',
           'MERCEDES-BENZ', 'FREIGHTLINER', 'SUBARU', 'ITALIKA', 'BAJAJ',
           'SATURN', 'MITSUBISHI', 'ISUZU', 'VOLKSWAGEN', 'MINI',
           'LAND ROVER', 'SCION', 'XCMG', 'BMW', 'AUDI', 'MITSUBISHI FUSO',
           'GMC', 'INTERNATIONAL', 'POLARIS', 'DODGE', 'SERPENTO', 'OSBORNE',
           'VOLVO', 'JOHN DEERE', 'ACURA', 'JAC', 'HARLEY-DAVIDSON', 'DUCATI',
           'ASIA HERO', 'UD NISSAN', 'PETERBILT', 'TRAILMOBILE', 'TVS',
           'LIUGONG', 'CHANGAN', 'FIAT', 'STRICK', 'HONDA FIT', 'UTILITY',
           'HOBBS', 'AZTEC', 'CADILLAC', 'CASE', 'HUMMER', 'INFINITI',
           'NEW HOLLAND', 'DORSEY', 'KENWORTH', 'PINES', 'KTM', 'PONTIAC',
           'WABASH', 'GEO', 'MCI', 'LEXUS', 'JCB', 'WRANGLER', 'SCOOTER',
           'PORSCHE', 'FREEDOM', 'CHRYSLER', 'CFMOTO', 'LOADCRAFT', 'GINDY',
           'HERCULES', 'BLUE BIRD', 'MASSEY FERGUSON', 'ZHONGYU', 'BOYD',
           'NEW FLYER', 'PIAGGIO', 'EAST', 'VANHOOL', 'ZHONGNENG', 'BETA',
           'MRT', 'STERLING', 'MASERATI', 'TRANSCRAFT', 'BIG DOG', 'HAOJUE',
           'CATERPILLAR', 'MACK', 'EDASAINFRA', 'HINO', 'FONTAINE', 'DAMON',
           'HISUN', 'ME', 'JAGUAR', 'RAM', 'TOMBERLIN', 'STOUGHTON', 'VENTO',
           'HERO', 'COLOVE', 'KEYSTONE', 'ZHEJIANG', 'FRUEHAUF', 'DAEWOO',
           'SSANG YONG', 'NUVAN', 'GREAT DANE', 'BOBCAT', 'CAN-AM',
           'STEEL WORKS', 'THOMAS', 'CLUB CAR', 'TRAIL', 'RENAULT', 'VESPA',
           'MVS', 'BOONE', 'TAOTAO', 'LEDWELL', 'SHOPMADE', 'COTTRELL',
           'PACIFIC', 'SIN MARCA', 'VORTEX', 'TRIUMPH', 'HUSQVARNA',
           'SHINERAY', 'DFSK', 'GAS-GAS', 'BAJA', 'BENELLI', 'OSHKOSH',
           'KYMCO', 'AVANTI', 'WHITE GMC', 'FERRARI', 'INGERSOLL RAND',
           'TRAIL KING', 'AKT', 'COLE', 'INDIAN', 'SHERCO', 'MG', 'KOMATSU',
           'SSR', 'LIFAN', 'DONG FANG'], dtype=object)




```python
dfImportacionS3["Marca"].unique()
```




    array(['TVS', 'HONDA', 'TOYOTA', 'PEUGEOT', 'SUZUKI', 'MAZDA', 'FORD',
           'FREIGHTLINER', 'BAJAJ', 'NISSAN', 'ISUZU', 'MITSUBISHI', 'HINO',
           'PETERBILT', 'UTILITY', 'PACER', 'INTERNATIONAL', 'JEEP',
           'VOLKSWAGEN', 'BMW', 'CHEVROLET', 'KIA', 'SCION', 'MINI', 'MOVESA',
           'JAC', 'HYUNDAI', 'STRICK', 'SERPENTO', 'STERLING', 'KENWORTH',
           'YAMAHA', 'HUMMER', 'KAWASAKI', 'FREEDOM', 'MACK', 'SUBARU',
           'MERCEDES-BENZ', 'PORSCHE', 'SINOTRUK', 'EDASAINFRA', 'RAM',
           'MONON', 'LAND ROVER', 'LEXUS', 'AUDI', 'HERO', 'ITALIKA', 'CASE',
           'ETNYRE', 'CONTI', 'HARLEY-DAVIDSON', 'SSANG YONG', 'LUFKIN',
           'UAZ', 'DODGE', 'GMC', 'ROAD SYSTEMS', 'VOLVO', 'FRUEHAUF',
           'POLARIS', 'DORSEY', 'UD NISSAN', 'MITSUBISHI FUSO', 'MCI', 'EAST',
           'BUICK', 'WABASH', 'CATERPILLAR', 'KTM', 'HUSQVARNA', 'CHRYSLER',
           'GAS-GAS', 'DUCATI', 'MG', 'BIRMINGHAM', 'TRAILMOBILE', 'GENESIS',
           'HAOJUE', 'KYMCO', 'JOHN DEERE', 'GREAT DANE', 'COTTRELL',
           'DONG FENG', 'TAOTAO', 'GEO', 'CFMOTO', 'FONTAINE', 'BAJA',
           'BLUE BIRD', 'LOADCRAFT', 'HERCULES', 'NO EXISTE', 'GSCR',
           'PONTIAC', 'INDIAN', 'WHITE', 'TRANSCRAFT', 'FUSO', 'WESTERN STAR',
           'JET COMPANY', 'YUNNEI POWER', 'ZONGSHEN', 'SMART', 'CHINA',
           'MANAC', 'SIN MARCA', 'ARTIC CAT', 'S', 'MCCORMICK', 'DAEWOO',
           'TRIUMPH', 'SUMT', 'ATV', 'BOMBARDIER LTD', 'HOMEMADE', 'OSBORNE',
           'ASIA HERO', 'STOUGHTON', 'JONWAY', 'INFINITI', 'KAZUMA', 'KINLON',
           'APRILIA', 'ARMOR', 'TRINITY', 'INTERSTATE', 'WULING', 'CHANGAN',
           'GALLEGOS', 'BMS', 'ACURA', 'WITZCOCHALLENGER', 'PIAGGIO',
           'CAN-AM', 'MV AGUSTA', 'SHERCO', 'SANYANG', 'HY0SUNG', 'ALLOY',
           'HYOSUNG', 'FOTON', 'MILLER', 'APOLO', 'TMR', 'E-Z-GO TEXTRON',
           'MASSEY FERGUSON', 'SATURN', 'ARCTIC CAT', 'XINGYUE', 'TRAIL',
           'VOLCON', 'JAGUAR', 'FONTAIN', 'VIKING', 'NABORS', 'HEIL', 'BOYD',
           'TRANCRAFT', 'LUFK', 'MASERATI', 'BERING', 'MERCURY',
           'NEW HOLLAND', 'IMPER', 'MILLENNIUM TRAILERS INC', 'BCX', 'SSR',
           'VESPA', 'CADILLAC', 'T-KING', 'SKYTEAM', 'E-TON', 'DACO',
           'MATLOCK', 'WARD', 'BENELLI', 'STRIK', 'AUTOCAR', 'PREVOST',
           'GALION'], dtype=object)



##### Valores distintos en la columna Linea


```python
dfImportacionRDS["Linea"].unique()
```




    array(['T110C', 'NAVI 110', 'MATRIX', ..., 'TACOMA TRD SPORT D/C 4WD',
           'TRX420TM1 RANCHER 4X2', 'XL7 4WD'], dtype=object)




```python
dfImportacionS3["Linea"].unique()
```




    array(['APACHE RTR 160', 'CR-V EX 4WD', 'COROLLA', ..., 'TRX250TM RECON',
           'TACOMA DO/CAB SR5 4WD', 'CR-V REAL TIME 4WD'], dtype=object)



##### Valores distintos columna Tipo importador


```python
dfImportacionRDS["Tipo de Importador"].unique()
```




    array(['DISTRIBUIDOR', 'OCASIONAL'], dtype=object)




```python
dfImportacionS3["Tipo de Importador"].unique()
```




    array(['DISTRIBUIDOR', 'OCASIONAL'], dtype=object)



##### Relación entre columna Linea Marca


```python
dfImportacionRDS.groupby(["Linea"])["Marca"].count()
```




    Linea
    1 TON 2WD             1
    1 TON LONG BED 2WD    2
    1085 4X2              1
    125 RR-S CHALEE       1
    128i COUPE            1
                         ..
    xA                    3
    xA HATCHBACK          1
    xB                    8
    xD                    5
    xD HATCHBACK          1
    Name: Marca, Length: 2813, dtype: int64



##### Columna Distintivo


```python
dfImportacionRDS["Distintivo"].unique()
```




    array(['LIVIANO', 'PESADO', None], dtype=object)




```python
dfImportacionS3["Distintivo"].unique()
```




    array(['LIVIANO', 'PESADO', nan], dtype=object)



#### Análisis exploración

Como resultado de la exploración de los datos se pudo encontrar que para la imortación hecha desde S3, hay una columna que se puede descartar ya que no posee ningún valor. También se determinó que diferentes marcas pueden tener las mismas líneas, el distintivo en algunas ocasiones viene con un null, así como en la aduana de ingreso en ocasiones contiene valores incorrectos, por ejemplo: "vehiculos", en relación a las aduanas los datos obtenidos del archivo Aduandas alojado en S3, el nombre de la aduanda es del tipo "Aduana Puerto Barrarios" mientras que en RDS y el otro archivo en S3 tiene las aduanas como "Puerto Barrios", otra diferencia es columna "Es almacenadora" ya que las importaciones de vehículos no trae esta información y es una columna que para este caso no aporta mayor información por lo que puede ser eliminada.


## Modelo De Datos

Como resultado del análisis anterior se creará el siguiente modelo:
- dimensión Paises
- dimensión Aduanas
- dimensión Partida Arancelaria
- dimensión Fecha
- dimensión Tipo Importador
- dimensión Vehiculo
- tabla de hechos Importaciones

## Procesamiento

##### Uniendo los dataframe de importaciones


```python
dfImportaciones = pd.DataFrame()

dfImportaciones = dfImportaciones.append(dfImportacionRDS)

#Cambio de nombre columna Fecha de la Poliza en dfImportacionS3 para match con dfImportacionRDS
dfImportacionS3 = dfImportacionS3.rename(columns = {" Fecha de la Poliza" : "Fecha de la Poliza" })
dfImportaciones = dfImportaciones.append(dfImportacionS3.iloc[:,0:17])
dfImportaciones.count()
```

    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/3100517111.py:3: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfImportaciones = dfImportaciones.append(dfImportacionRDS)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/3100517111.py:7: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfImportaciones = dfImportaciones.append(dfImportacionS3.iloc[:,0:17])





    Pais de Proveniencia    76450
    Aduana de Ingreso       76450
    Fecha de la Poliza      76450
    Partida Arancelaria     76450
    Modelo del Vehiculo     76450
    Marca                   76450
    Linea                   76450
    Centimetros Cubicos     76436
    Distintivo              74873
    Tipo de Vehiculo        76450
    Tipo de Importador      76450
    Tipo Combustible        76450
    Asientos                76450
    Puertas                 76450
    Tonelaje                76450
    Valor CIF               76450
    Impuesto                76450
    dtype: int64




```python
dfImportaciones.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Pais de Proveniencia</th>
      <th>Aduana de Ingreso</th>
      <th>Fecha de la Poliza</th>
      <th>Partida Arancelaria</th>
      <th>Modelo del Vehiculo</th>
      <th>Marca</th>
      <th>Linea</th>
      <th>Centimetros Cubicos</th>
      <th>Distintivo</th>
      <th>Tipo de Vehiculo</th>
      <th>Tipo de Importador</th>
      <th>Tipo Combustible</th>
      <th>Asientos</th>
      <th>Puertas</th>
      <th>Tonelaje</th>
      <th>Valor CIF</th>
      <th>Impuesto</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CHINA</td>
      <td>PUERTO QUETZAL</td>
      <td>18/01/2022</td>
      <td>8711209000</td>
      <td>2022</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>DISTRIBUIDOR</td>
      <td>GASOLINA</td>
      <td>2</td>
      <td>0</td>
      <td>0.0</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
  </tbody>
</table>
</div>



##### Creando dataframe para las aduanas


```python
dfAduanas = pd.DataFrame()

dfAduanas = pd.concat([dfAduanas, dfAduanasS3["Aduanas"].str.upper()], axis = 1)
dfAduanas = dfAduanas.rename(columns = {"Aduanas": "Aduana"})
dfAduanas["Nombre Corto"] = [aduana.split("ADUANA")[1].strip() if len(aduana.split("ADUANA")) == 2 else aduana for aduana in dfAduanas["Aduana"]]
dfAduanas.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Aduana</th>
      <th>Nombre Corto</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ADUANA TECÚN UMÁN 1</td>
      <td>TECÚN UMÁN 1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ADUANA INTEGRADA AGUA CALIENTE</td>
      <td>INTEGRADA AGUA CALIENTE</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ADUANA INTEGRADA CORINTO</td>
      <td>INTEGRADA CORINTO</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ADUANA INTEGRADA EL FLORIDO</td>
      <td>INTEGRADA EL FLORIDO</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ADUANA PUERTO QUETZAL</td>
      <td>PUERTO QUETZAL</td>
    </tr>
  </tbody>
</table>
</div>




```python
dfTempAduanas = pd.DataFrame({"Aduanas" :dfImportaciones["Aduana de Ingreso"].unique()})

for item in dfTempAduanas["Aduanas"]:
    if(item  not in dfAduanas["Nombre Corto"].values and item not in dfAduanas["Aduana"].values):
        dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
        
dfAduanas
```

    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/1776890133.py:5: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/1776890133.py:5: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/1776890133.py:5: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/1776890133.py:5: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/1776890133.py:5: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/1776890133.py:5: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)
    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/1776890133.py:5: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dfAduanas =dfAduanas.append({"Aduana":item, "Nombre Corto":item}, ignore_index = True)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Aduana</th>
      <th>Nombre Corto</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ADUANA TECÚN UMÁN 1</td>
      <td>TECÚN UMÁN 1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ADUANA INTEGRADA AGUA CALIENTE</td>
      <td>INTEGRADA AGUA CALIENTE</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ADUANA INTEGRADA CORINTO</td>
      <td>INTEGRADA CORINTO</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ADUANA INTEGRADA EL FLORIDO</td>
      <td>INTEGRADA EL FLORIDO</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ADUANA PUERTO QUETZAL</td>
      <td>PUERTO QUETZAL</td>
    </tr>
    <tr>
      <th>5</th>
      <td>ADUANA SANTO TOMAS DE CASTILLA</td>
      <td>SANTO TOMAS DE CASTILLA</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ADUANA EXPRESS AEREO</td>
      <td>EXPRESS AEREO</td>
    </tr>
    <tr>
      <th>7</th>
      <td>ADUANA CENTRAL</td>
      <td>CENTRAL</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ADUANA TECUN UMAN</td>
      <td>TECUN UMAN</td>
    </tr>
    <tr>
      <th>9</th>
      <td>ADUANA PUERTO BARRIOS</td>
      <td>PUERTO BARRIOS</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ADUANA PEDRO DE ALVARADO</td>
      <td>PEDRO DE ALVARADO</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ADUANA SAN CRISTOBAL</td>
      <td>SAN CRISTOBAL</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ADUANA ALMACENADORA INTEGRADA (ALMACENADORA)</td>
      <td>ALMACENADORA INTEGRADA (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ADUANA AGUA CALIENTE</td>
      <td>AGUA CALIENTE</td>
    </tr>
    <tr>
      <th>14</th>
      <td>ADUANA EL FLORIDO</td>
      <td>EL FLORIDO</td>
    </tr>
    <tr>
      <th>15</th>
      <td>ADUANA ALPASA (ALMACENADORA)</td>
      <td>ALPASA (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>16</th>
      <td>ADUANA CENTRALSA (ALMACENADORA)</td>
      <td>CENTRALSA (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>17</th>
      <td>ADUANA LA ERMITA</td>
      <td>LA ERMITA</td>
    </tr>
    <tr>
      <th>18</th>
      <td>ADUANA EL CARMEN</td>
      <td>EL CARMEN</td>
    </tr>
    <tr>
      <th>19</th>
      <td>ADUANA VALLE NUEVO</td>
      <td>VALLE NUEVO</td>
    </tr>
    <tr>
      <th>20</th>
      <td>ADUANA ALCORSA (ALMACENADORA)</td>
      <td>ALCORSA (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>21</th>
      <td>ADUANA ALMINTER (ALMACENADORA)</td>
      <td>ALMINTER (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>22</th>
      <td>ADUANA ALSERSA (ALMACENADORA)</td>
      <td>ALSERSA (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>23</th>
      <td>ADUANA CEALSA (ALMACENADORA)</td>
      <td>CEALSA (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>24</th>
      <td>ADUANA ALMAGUATE (ALMACENADORA)</td>
      <td>ALMAGUATE (ALMACENADORA)</td>
    </tr>
    <tr>
      <th>25</th>
      <td>ADUANA CENTRAL DE AVIACION</td>
      <td>CENTRAL DE AVIACION</td>
    </tr>
    <tr>
      <th>26</th>
      <td>ADUANA MELCHOR DE MENCOS</td>
      <td>MELCHOR DE MENCOS</td>
    </tr>
    <tr>
      <th>27</th>
      <td>ADUANA FARDOS POSTALES</td>
      <td>FARDOS POSTALES</td>
    </tr>
    <tr>
      <th>28</th>
      <td>EL CEIBO</td>
      <td>EL CEIBO</td>
    </tr>
    <tr>
      <th>29</th>
      <td>ADUANA LA MESILLA</td>
      <td>LA MESILLA</td>
    </tr>
    <tr>
      <th>30</th>
      <td>TIKAL</td>
      <td>TIKAL</td>
    </tr>
    <tr>
      <th>31</th>
      <td>G8, CENTRALSA</td>
      <td>G8, CENTRALSA</td>
    </tr>
    <tr>
      <th>32</th>
      <td>G1, INTEGRADA</td>
      <td>G1, INTEGRADA</td>
    </tr>
    <tr>
      <th>33</th>
      <td>G4, ALSERSA</td>
      <td>G4, ALSERSA</td>
    </tr>
    <tr>
      <th>34</th>
      <td>G5, CEALSA</td>
      <td>G5, CEALSA</td>
    </tr>
    <tr>
      <th>35</th>
      <td>VEHICULOS</td>
      <td>VEHICULOS</td>
    </tr>
    <tr>
      <th>36</th>
      <td>G7, ALCORSA</td>
      <td>G7, ALCORSA</td>
    </tr>
    <tr>
      <th>37</th>
      <td>G3, ALPASA</td>
      <td>G3, ALPASA</td>
    </tr>
  </tbody>
</table>
</div>



### Creando las Dimensiones

##### Dimensión Aduanas


```python
dimAduana = pd.DataFrame(columns= ["aduana_sk"])

dimAduana = dimAduana.append(dfAduanas)
dimAduana = dimAduana.rename(columns = {"Aduana":"aduana", "Nombre Corto":"nombre_corto"})
dimAduana["aduana_sk"] = range(1, len(dimAduana) + 1)
dimAduana.head()
```

    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/3749969793.py:3: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      dimAduana = dimAduana.append(dfAduanas)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>aduana_sk</th>
      <th>aduana</th>
      <th>nombre_corto</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>ADUANA TECÚN UMÁN 1</td>
      <td>TECÚN UMÁN 1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>ADUANA INTEGRADA AGUA CALIENTE</td>
      <td>INTEGRADA AGUA CALIENTE</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>ADUANA INTEGRADA CORINTO</td>
      <td>INTEGRADA CORINTO</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>ADUANA INTEGRADA EL FLORIDO</td>
      <td>INTEGRADA EL FLORIDO</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>ADUANA PUERTO QUETZAL</td>
      <td>PUERTO QUETZAL</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimensión Fecha


```python
dimFecha = pd.DataFrame()

dimFecha = pd.concat([dimFecha, dfImportaciones["Fecha de la Poliza"]], axis = 1)
dimFecha = dimFecha.rename(columns = {"Fecha de la Poliza": "fecha"})
dimFecha = dimFecha.drop_duplicates(subset = "fecha")
dimFecha["fecha"] = pd.to_datetime(dimFecha["fecha"], format = "%d/%m/%Y")
dimFecha.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fecha</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2022-01-18</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2022-01-19</td>
    </tr>
    <tr>
      <th>44</th>
      <td>2022-01-26</td>
    </tr>
    <tr>
      <th>54</th>
      <td>2022-01-27</td>
    </tr>
    <tr>
      <th>210</th>
      <td>2022-02-07</td>
    </tr>
  </tbody>
</table>
</div>




```python
dimFecha["anio"] = pd.DatetimeIndex(dimFecha["fecha"]).year
dimFecha["mes"] = pd.DatetimeIndex(dimFecha["fecha"]).month
dimFecha["trimestre"] = pd.DatetimeIndex(dimFecha["fecha"]).quarter
dimFecha["dia"] = pd.DatetimeIndex(dimFecha["fecha"]).day
dimFecha["semana"] = pd.DatetimeIndex(dimFecha["fecha"]).week
dimFecha["dia_semana"] = pd.DatetimeIndex(dimFecha["fecha"]).dayofweek
dimFecha.head()
```

    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/2673028398.py:5: FutureWarning: weekofyear and week have been deprecated, please use DatetimeIndex.isocalendar().week instead, which returns a Series. To exactly reproduce the behavior of week and weekofyear and return an Index, you may call pd.Int64Index(idx.isocalendar().week)
      dimFecha["semana"] = pd.DatetimeIndex(dimFecha["fecha"]).week





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fecha</th>
      <th>anio</th>
      <th>mes</th>
      <th>trimestre</th>
      <th>dia</th>
      <th>semana</th>
      <th>dia_semana</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2022-01-18</td>
      <td>2022</td>
      <td>1</td>
      <td>1</td>
      <td>18</td>
      <td>3</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2022-01-19</td>
      <td>2022</td>
      <td>1</td>
      <td>1</td>
      <td>19</td>
      <td>3</td>
      <td>2</td>
    </tr>
    <tr>
      <th>44</th>
      <td>2022-01-26</td>
      <td>2022</td>
      <td>1</td>
      <td>1</td>
      <td>26</td>
      <td>4</td>
      <td>2</td>
    </tr>
    <tr>
      <th>54</th>
      <td>2022-01-27</td>
      <td>2022</td>
      <td>1</td>
      <td>1</td>
      <td>27</td>
      <td>4</td>
      <td>3</td>
    </tr>
    <tr>
      <th>210</th>
      <td>2022-02-07</td>
      <td>2022</td>
      <td>2</td>
      <td>1</td>
      <td>7</td>
      <td>6</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimensión Tipo Importador


```python
dimTipoImportador = pd.DataFrame(columns = ["tipo_importador_sk"])

dimTipoImportador = pd.concat([dimTipoImportador, dfImportaciones["Tipo de Importador"].drop_duplicates()])
dimTipoImportador = dimTipoImportador.rename(columns = {0:"tipo_importador"})
dimTipoImportador["tipo_importador_sk"] = range(1, len(dimTipoImportador)+1)
dimTipoImportador.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>tipo_importador_sk</th>
      <th>tipo_importador</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>DISTRIBUIDOR</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2</td>
      <td>OCASIONAL</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimensión Partida arancelaria


```python
dimPartidaArancelaria = pd.DataFrame(columns = ["partida_arancelaria_sk"])

dimPartidaArancelaria = pd.concat([dimPartidaArancelaria, dfImportaciones["Partida Arancelaria"]])
dimPartidaArancelaria = dimPartidaArancelaria.rename(columns = {0: "partida_arancelaria"})
dimPartidaArancelaria["partida_arancelaria"] = pd.to_numeric(dimPartidaArancelaria["partida_arancelaria"], downcast = "integer")
dimPartidaArancelaria = dimPartidaArancelaria.drop_duplicates()
dimPartidaArancelaria["partida_arancelaria_sk"] = range(1, len(dimPartidaArancelaria)+1)
dimPartidaArancelaria.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>partida_arancelaria_sk</th>
      <th>partida_arancelaria</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>8711209000</td>
    </tr>
    <tr>
      <th>44</th>
      <td>2</td>
      <td>8703236991</td>
    </tr>
    <tr>
      <th>45</th>
      <td>3</td>
      <td>8704215900</td>
    </tr>
    <tr>
      <th>52</th>
      <td>4</td>
      <td>8703326991</td>
    </tr>
    <tr>
      <th>53</th>
      <td>5</td>
      <td>8703237991</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimensión Países


```python
dimPais = pd.DataFrame(columns = ["pais_sk"])

dimPais = pd.concat([dimPais, dfImportaciones["Pais de Proveniencia"].drop_duplicates()])
dimPais = dimPais.rename(columns = {0: "pais"})
dimPais["pais_sk"] = range(1, len(dimPais) + 1)
dimPais.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>pais_sk</th>
      <th>pais</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>CHINA</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2</td>
      <td>INDIA</td>
    </tr>
    <tr>
      <th>44</th>
      <td>3</td>
      <td>CANADA</td>
    </tr>
    <tr>
      <th>45</th>
      <td>4</td>
      <td>ARGENTINA</td>
    </tr>
    <tr>
      <th>52</th>
      <td>5</td>
      <td>COREA DEL SUR</td>
    </tr>
  </tbody>
</table>
</div>



##### Dimensión Vehiculos


```python
dimVehiculo = pd.DataFrame(columns = ["vehiculo_sk"])

dimVehiculo = pd.concat([dimVehiculo, dfImportaciones.loc[:,["Modelo del Vehiculo", "Marca", "Linea", "Centimetros Cubicos", "Distintivo", 
                                                      "Tipo de Vehiculo", "Tipo Combustible", "Asientos", "Puertas", "Tonelaje"]]])
dimVehiculo = dimVehiculo.rename(columns = {"Modelo del Vehiculo": "modelo", "Marca":"marca", "Linea":"linea", "Centimetros Cubicos" : "centimetros_cubicos",
                                         "Distintivo":"distintivo", "Tipo de Vehiculo": "tipo", "Tipo Combustible": "combustible", "Asientos": "asientos",
                                         "Puertas": "puertas", "Tonelaje": "tonelaje"})
dimVehiculo = dimVehiculo.drop_duplicates()
dimVehiculo["vehiculo_sk"] = range(1, len(dimVehiculo) +1)
dimVehiculo.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>vehiculo_sk</th>
      <th>modelo</th>
      <th>marca</th>
      <th>linea</th>
      <th>centimetros_cubicos</th>
      <th>distintivo</th>
      <th>tipo</th>
      <th>combustible</th>
      <th>asientos</th>
      <th>puertas</th>
      <th>tonelaje</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2022.0</td>
      <td>YAMAHA</td>
      <td>T110C</td>
      <td>110.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>GASOLINA</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2</td>
      <td>2021.0</td>
      <td>HONDA</td>
      <td>NAVI 110</td>
      <td>109.0</td>
      <td>LIVIANO</td>
      <td>MOTO</td>
      <td>GASOLINA</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>44</th>
      <td>3</td>
      <td>2005.0</td>
      <td>TOYOTA</td>
      <td>MATRIX</td>
      <td>1800.0</td>
      <td>LIVIANO</td>
      <td>CAMIONETILLA</td>
      <td>GASOLINA</td>
      <td>5.0</td>
      <td>4.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>45</th>
      <td>4</td>
      <td>2022.0</td>
      <td>TOYOTA</td>
      <td>HILUX</td>
      <td>2393.0</td>
      <td>LIVIANO</td>
      <td>PICK UP</td>
      <td>DIESEL</td>
      <td>3.0</td>
      <td>2.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>52</th>
      <td>5</td>
      <td>2007.0</td>
      <td>KIA</td>
      <td>SPORTAGE TLX 2WD</td>
      <td>2000.0</td>
      <td>LIVIANO</td>
      <td>CAMIONETA</td>
      <td>DIESEL</td>
      <td>5.0</td>
      <td>5.0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>



### Creando la Tabla de Hechos


```python
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
```

    /var/folders/hh/25c_zdz92t9c70mlh69s5tg80000gn/T/ipykernel_887/614960734.py:3: FutureWarning: The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.
      factImportaciones = factImportaciones.append(dfImportaciones)





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>fecha</th>
      <th>pais_sk</th>
      <th>aduana_sk</th>
      <th>partida_arancelaria_sk</th>
      <th>tipo_importador_sk</th>
      <th>vehiculo_sk</th>
      <th>valor_cif</th>
      <th>impuesto</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2022-01-18</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2022-01-18</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2022-01-18</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2022-01-18</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2022-01-18</td>
      <td>1</td>
      <td>5</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>767777.85</td>
      <td>92133.34</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>76403</th>
      <td>2022-03-07</td>
      <td>7</td>
      <td>5</td>
      <td>113</td>
      <td>2</td>
      <td>9251</td>
      <td>324070.82</td>
      <td>38888.50</td>
    </tr>
    <tr>
      <th>76404</th>
      <td>2022-03-07</td>
      <td>7</td>
      <td>5</td>
      <td>113</td>
      <td>2</td>
      <td>9251</td>
      <td>324070.67</td>
      <td>38888.48</td>
    </tr>
    <tr>
      <th>76405</th>
      <td>2022-03-07</td>
      <td>7</td>
      <td>5</td>
      <td>113</td>
      <td>2</td>
      <td>9251</td>
      <td>324070.67</td>
      <td>38888.48</td>
    </tr>
    <tr>
      <th>76406</th>
      <td>2022-02-28</td>
      <td>6</td>
      <td>6</td>
      <td>112</td>
      <td>2</td>
      <td>8877</td>
      <td>404665.71</td>
      <td>48559.89</td>
    </tr>
    <tr>
      <th>76407</th>
      <td>2022-02-28</td>
      <td>6</td>
      <td>6</td>
      <td>112</td>
      <td>2</td>
      <td>9006</td>
      <td>405087.08</td>
      <td>48610.45</td>
    </tr>
  </tbody>
</table>
<p>76408 rows × 8 columns</p>
</div>



### Cargando a Redshift


```python
connection_engine = create_engine(redshift_connection)
```


```python
#Insert de las dimensiones y tabla de hechos
dimAduana.to_sql("dim_aduana", connection_engine, index = False, if_exists = "append")
dimFecha.to_sql("dim_fecha", connection_engine, index = False, if_exists = "append", method="multi")
dimTipoImportador.to_sql("dim_tipo_importador", connection_engine, index = False, if_exists = "append")
dimPartidaArancelaria.to_sql("dim_partida_arancelaria", connection_engine, index = False, if_exists = "append", method="multi")
dimPais.to_sql("dim_pais", connection_engine, index = False, if_exists = "append", method="multi")
dimVehiculo.to_sql("dim_vehiculo", connection_engine, index = False, if_exists = "append", method="multi")
factImportaciones.to_sql("fact_importaciones", connection_engine, index = False, if_exists = "append", method="multi")
```




    76408



## Analitica

##### ¿Cuál fue el monto recaudado en impuesto para enero y febrero de 2022?


```python
sum(factImportaciones["impuesto"])
```




    8297207141.788323



El impuesto recaudado para enero y febrero 2022 es de Q. 8,297,207,141.79

##### ¿Cuánto es el impuesto recaudado por día?


```python
factImportaciones.groupby("fecha")["impuesto"].sum()
```




    fecha
    2022-01-08    5.954568e+05
    2022-01-09    5.410548e+04
    2022-01-10    1.482876e+08
    2022-01-11    2.084106e+08
    2022-01-12    1.519079e+08
    2022-01-13    9.263677e+07
    2022-01-14    2.855507e+08
    2022-01-15    7.303961e+05
    2022-01-16    1.362530e+05
    2022-01-17    7.940829e+07
    2022-01-18    6.323541e+08
    2022-01-19    2.571232e+08
    2022-01-20    1.978454e+07
    2022-01-21    1.640633e+08
    2022-01-22    8.990819e+05
    2022-01-23    2.743252e+05
    2022-01-24    3.005043e+07
    2022-01-25    4.441352e+08
    2022-01-26    7.356601e+08
    2022-01-27    4.271329e+07
    2022-01-28    3.217900e+08
    2022-01-29    1.701847e+07
    2022-01-30    4.126262e+05
    2022-01-31    5.403526e+07
    2022-02-01    2.276849e+08
    2022-02-02    2.933588e+08
    2022-02-03    1.456622e+07
    2022-02-04    1.414441e+07
    2022-02-05    1.945745e+07
    2022-02-06    5.021689e+05
    2022-02-07    1.631682e+08
    2022-02-08    3.188953e+08
    2022-02-09    5.583870e+08
    2022-02-10    3.248286e+08
    2022-02-11    1.531459e+08
    2022-02-12    1.659576e+07
    2022-02-13    4.053793e+05
    2022-02-14    5.109971e+07
    2022-02-15    1.078875e+08
    2022-02-16    2.193177e+06
    2022-02-17    2.465202e+06
    2022-02-18    4.945058e+07
    2022-02-19    1.150546e+06
    2022-02-20    4.214860e+05
    2022-02-21    2.274169e+08
    2022-02-22    5.531984e+07
    2022-02-23    1.643508e+08
    2022-02-24    4.030091e+08
    2022-02-25    1.170678e+07
    2022-02-26    1.045488e+06
    2022-02-27    4.061960e+05
    2022-02-28    1.046704e+07
    2022-03-01    8.225680e+08
    2022-03-02    5.240608e+07
    2022-03-03    7.443832e+07
    2022-03-04    3.653698e+08
    2022-03-05    8.009289e+07
    2022-03-06    6.420812e+05
    2022-03-07    2.012748e+07
    Name: impuesto, dtype: float64



##### ¿Cuál el valor promedio de la importaciones?


```python
factImportaciones["valor_cif"].mean()
```




    904913.0542316281



El valor CIF promedio de las importaciones es de Q. 904,913.05

##### ¿Cuál el monto de la importación de mayor valor?


```python
factImportaciones["valor_cif"].max()
```




    5199182.49



La importación de mayor valor es de Q. 5,199,182.49

##### ¿Cuál es el monto de la importación de menor valor?


```python
factImportaciones["valor_cif"].min()
```




    1964.84



La importación de menor valor es de Q. 1,964.84

##### ¿Cuánto es el país de donde más importa?


```python
dfTemp = pd.DataFrame()
dfTemp = pd.concat([dfTemp,factImportaciones["pais_sk"]], axis =1)
dfTemp = dfTemp.merge(dimPais, left_on="pais_sk", right_on="pais_sk")
dfTemp.groupby("pais")["pais_sk"].count().sort_values(ascending= False)
```




    pais
    CHINA                 32465
    INDIA                 20486
    JAPON                  9856
    ESTADOS UNIDOS         5272
    COREA DEL SUR          2042
    CANADA                 1491
    MEXICO                 1127
    TAILANDIA               873
    BRASIL                  572
    ARGENTINA               486
    REINO UNIDO             457
    ALEMANIA REP. FED.      360
    INDONESIA               313
    TAIWAN                  215
    AUSTRIA                 117
    FRANCIA                  74
    ITALIA                   73
    BELICE                   21
    SUECIA                   20
    HUNGRIA                  16
    TURQUIA                  16
    IRLANDA                  15
    SLOVAKIA                 11
    BELGICA                   8
    AUSTRALIA                 5
    ESPANA                    5
    SUDAFRICA                 3
    SUIZA                     3
    PANAMA                    2
    COMOROS                   1
    KENYA                     1
    COLOMBIA                  1
    U.R.S.S.                  1
    Name: pais_sk, dtype: int64



El país de donde más se importan vehículos es China

##### ¿Cuántos vehiculos ingresan por aduana?


```python
dfTemp = pd.DataFrame()
dfTemp = pd.concat([dfTemp,factImportaciones["aduana_sk"]], axis =1)
dfTemp = dfTemp.merge(dimAduana, left_on="aduana_sk", right_on="aduana_sk")
dfTemp.groupby("aduana")["aduana_sk"].count().sort_values(ascending= False)
```




    aduana
    ADUANA PUERTO QUETZAL             57571
    ADUANA EL CARMEN                   6352
    ADUANA TECUN UMAN                  4770
    ADUANA SANTO TOMAS DE CASTILLA     3192
    ADUANA PUERTO BARRIOS              2478
    G8, CENTRALSA                       902
    G1, INTEGRADA                       711
    ADUANA PEDRO DE ALVARADO            294
    ADUANA MELCHOR DE MENCOS             36
    G4, ALSERSA                          19
    G7, ALCORSA                          19
    G3, ALPASA                           18
    VEHICULOS                            16
    ADUANA LA MESILLA                     8
    EL CEIBO                              6
    G5, CEALSA                            6
    ADUANA SAN CRISTOBAL                  5
    ADUANA VALLE NUEVO                    4
    ADUANA LA ERMITA                      1
    Name: aduana_sk, dtype: int64



La aduana donde ingresan más vehículos es la Aduana de Puerto Quetzal


```python

```
