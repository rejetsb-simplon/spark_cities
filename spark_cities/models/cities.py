class Cities:
  CODE_INSEE = "code_commune_insee"
  NOM_COMMUNE = "nom_de_la_commune"
  CODE_POSTAL = "code_postal"
  LIGNE_5 = "ligne_5"
  LB_ACHEMINEMENT = "libelle_d_acheminement"
  COORDONNEES_GPS = "coordonnees_gps"
  LAT = "latitude"
  LON = "longitude"
  DEPT = "dept"
  NB_VILLES = "nombre de communes"
  NOM_PREF = "nom_prefecture"
  LAT_PREF = "latitude_prefecture"
  LON_PREF = "longitude_prefecture"
  DIST = "distance_prefecture"
  DIST_MOY = "distance_moyenne_prefecture_dept"
  DIST_MED = "distance_mediane_prefecture_dept"
  
  @staticmethod
  def read_csv(spark):
    # return spark.read.csv("hdfs://localhost:9000/data/raw/cities/v1/csv",header=True, sep=";")
    return spark.read.csv("/data/raw/cities/v1/csv",header=True, sep=";")

  @staticmethod
  def write_parquet(df,path):
    # df.write.mode("overwrite").parquet("hdfs://"+path)
    df.write.mode("overwrite").parquet(path)

  @staticmethod
  def write_csv(df,path,type='distribue'):
    # df.write.mode("overwrite").csv("hdfs://"+path)
    if type == "unique":
      df.coalesce(1).write.mode("overwrite").csv(path,header=True,sep=';')
    else:
      df.write.mode("overwrite").csv(path,header=True,sep=';')
