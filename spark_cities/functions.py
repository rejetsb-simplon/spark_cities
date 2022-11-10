from pyspark.sql import functions as F, types as T
from spark_cities.models.cities import Cities
import math
import numpy as np

def split_lat_long(df):
  # Extraire la latitude et la longitude a partir de la column gps
  # Les donnees de latitude doivent etre de type double.
  # La colonne coordonnees_gps initiale ne doit plus exister dans le dataframe de sortie.
  return df \
    .withColumn(Cities.LAT, (F.substring_index(F.col(Cities.COORDONNEES_GPS),',',1)).cast('double')) \
    .withColumn(Cities.LON, (F.substring_index(F.col(Cities.COORDONNEES_GPS),',',-1)).cast('double')) \
    .drop(F.col(Cities.COORDONNEES_GPS))
  #return df \
  #  .withColumn(Cities.LAT, (F.split(F.col(Cities.COORDONNEES_GPS),',').getitem(0)).cast('double')) \
  #  .withColumn(Cities.LON, (F.split(F.col(Cities.COORDONNEES_GPS),',').getitem(1)).cast('double')) \
  #  .drop(F.col(Cities.COORDONNEES_GPS))

def departement(strcol):
  # Extraire les deux premiers chiffres du code postal dans une colonne dept
  # Gestion sur la colonne
  return F.substring(strcol,1,2)

def departement_return_df(df):
  # Extraire les deux premiers chiffres du code postal dans une colonne dept
  # Gestion sur le dataframe
  return df.withColumn(Cities.DEPT, F.substring(df[Cities.CODE_POSTAL],1,2))

# DEPARTEMENT_UDF (que sur colonne et pas sur df)
# Ecriture function en PYTHON
@F.udf(returnType=T.StringType())
def departement_udf(strcol):
  if strcol[:2] != '20':
    return strcol[:2]
  elif int(strcol[:3]) < 202:
    return '2A'
  else:
    return '2B'

# DEPARTEMENT_FCT
def departement_fct(df):
  # Extraire les deux premiers chiffres du code postal dans une colonne dept
  return F.when(F.substring(df[Cities.CODE_INSEE],1,2).isin(['2A','2B']), \
    F.substring(df[Cities.CODE_INSEE],1,2)) \
      .otherwise(F.substring(df[Cities.CODE_POSTAL],1,2))

def departement_fct_return_df(df):
  # Extraire les deux premiers chiffres du code postal dans une colonne dept
  # Gestion sur le dataframe
  df1 = df.filter( \
    (F.substring(df[Cities.CODE_INSEE],1,2) != '2A') \
      & (F.substring(df[Cities.CODE_INSEE],1,2) != '2B') \
        ) \
          .withColumn(Cities.DEPT, F.substring(df[Cities.CODE_POSTAL],1,2))
  df2 = df.filter( \
    (F.substring(df[Cities.CODE_INSEE],1,2).isin(['2A','2B']))
    ) \
      .withColumn(Cities.DEPT, F.substring(df[Cities.CODE_INSEE],1,2))
  return df1.unionByName(df2)

# Distance GPS
@F.udf(returnType=T.DoubleType())
def distance_udf(coord1, coord2):
  try:
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    lat = (lat2 - lat1)**2
    lon = (lon2 - lon1)**2
    res = lat+lon
    return math.sqrt(res)
  except Exception:
    return 0

@F.udf(returnType=T.DoubleType())
def find_median_udf(values_list):
  median = np.median(values_list)
  return round(float(median),2)
