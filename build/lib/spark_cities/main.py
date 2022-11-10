import pyspark.sql.functions as F
from spark_cities.models.cities import Cities
from spark_cities.functions import *
from pyspark.sql import Window

def main(spark):
    # Creer un dataframe de cities en utilisant le fichier csv cities
    df=Cities.read_csv(spark)

    # Ecrire le dataframe de cities dans HDFS au format parquet dans /experiment/cities/v1/parquet
    # Faire en sorte que les donnees soient ecrasees si on relance notre application
    Cities.write_parquet(df,"/experiment/cities/v1/parquet")

    # Creer un nouveau dataframe cities_clean qui contiend uniquement les colonnes suivantes
    # code postal, code insee, nom de la ville, coordonnees gps
    df1 = df.select(df[Cities.CODE_POSTAL],df[Cities.CODE_INSEE], \
        df[Cities.NOM_COMMUNE],df[Cities.COORDONNEES_GPS])

    # Creer une fonction split_lat_long qui prend en entree un dataframe et qui renvoie un dataframe.
    # La fonction doit transformer la colonne coordonnees_gps en deux colonnes latitude et longitude.
    df2=split_lat_long(df1)

    # Creer une colonne 'dept' qui contient les deux premiers chiffres du code postal.
    # La colonne doit etre de type string
    cities_clean = df2.withColumn(Cities.DEPT, departement(df[Cities.CODE_POSTAL]))
    #cities_clean = departement_return_df(df2)

    # Ecrire le dataframe de cities dans HDFS au format parquet dans /experiment/cities/v1/parquet
    # Faire en sorte que les donnees soient ecrasees si on relance notre application
    Cities.write_parquet(cities_clean,"/refined/cities/v1/parquet")

    # Creer un dataframe contenant le nombre de communes par departement
    # Trie par ordre decroissant de compte
    # (le departement contenant le plus de villes doit etre sur la premiere ligne)
    df_dept=cities_clean.groupby(Cities.DEPT).count() \
        .withColumnRenamed('count', Cities.NB_VILLES) \
            .orderBy(F.desc(Cities.NB_VILLES))
    # Sauvegarder le resultat dans un fichier csv unique (gestion par coalesce())
    # sur hdfs au format csv dans le dossier /refined/departement/v1/csv
    Cities.write_csv(df_dept,'/refined/departement/v1/csv')

    # Creer un dataframe contenant le nombre de communes par departement
    # Trie par ordre decroissant de compte
    # (le departement contenant le plus de villes doit etre sur la premiere ligne)
    # En gerant le cas de la Corse
    
    # Avec UDF
    cities_clean2 = df2.withColumn(Cities.DEPT, departement_udf(df[Cities.CODE_POSTAL]))
    df_dept2=cities_clean2.groupby(Cities.DEPT).count() \
        .withColumnRenamed('count', Cities.NB_VILLES) \
            .orderBy(F.desc(Cities.NB_VILLES))
    # Sauvegarder le resultat dans un fichier csv
    # sur hdfs au format csv dans le dossier /refined/departement/v2/csv
    Cities.write_csv(df_dept2,'/refined/departement/v2/csv')

    # Sans UDF
    cities_clean3 = df2.withColumn(Cities.DEPT, departement_fct(df2))
    #cities_clean3=departement_fct_return_df(df2)
    df_dept3=cities_clean3.groupby(Cities.DEPT).count() \
        .withColumnRenamed('count', Cities.NB_VILLES) \
            .orderBy(F.desc(Cities.NB_VILLES))

    # window function
    # A chaque ville ajouter les coordonnees GPS de la prefecture du departement.
    # La prefecture du departement se situe dans la ville ayant le code postal le plus petit
    # dans tout le departement.
    # Pour l exercice on considere egalement que la Corse est un seul departement
    window = Window.partitionBy(Cities.DEPT).orderBy(Cities.CODE_POSTAL)
    nom_prefecture = F.first(Cities.NOM_COMMUNE).over(window)
    lat_prefecture = F.first(Cities.LAT).over(window)
    lon_prefecture = F.first(Cities.LON).over(window)
    cities_clean4 = cities_clean2 \
        .withColumn(Cities.NOM_PREF, nom_prefecture) \
        .withColumn(Cities.LAT_PREF, lat_prefecture.cast('double')) \
        .withColumn(Cities.LON_PREF, lon_prefecture.cast('double'))

    # Une fois la prefecture trouvee, calculer la distance relative de chaque ville
    # par rapport a la prefecture.
    cities_clean5 = cities_clean4 \
        .withColumn(Cities.DIST, \
            distance_udf( \
                F.struct(cities_clean4[Cities.LAT],cities_clean4[Cities.LON]), \
                F.struct(cities_clean4[Cities.LAT_PREF],cities_clean4[Cities.LON_PREF]) \
            ))

    # Calculer la distance moyenne et mediane a la prefecture par departement
    window=Window.partitionBy(Cities.DEPT)
    dist_moy=F.avg(Cities.DIST).over(window)
    # percentile valable uniquement en pyspark3
    dist_med=F.percentile_approx(Cities.DIST,0.5).over(window)
    cities_clean6 = cities_clean5 \
        .withColumn(Cities.DIST_MOY,F.round(dist_moy,2)) \
        .withColumn(Cities.DIST_MED,F.round(dist_med,2))
    # Cela cree un nouveau dataframe avec colonnes :
    #    dept, LIST_DIST=liste des ditances, distance mediane
    # cities_clean7 = cities_clean5.groupBy(Cities.DEPT) \
    #     .agg(F.collect_list(Cities.DIST).alias("LST_DIST")) \
    #     .withColumn(Cities.DIST_MED,find_median_udf("LST_DIST"))
    # cities_clean6 = cities_clean6.withColumn(Cities.DIST_MOY, F.lit(None))

    # Sauvegarder le resultat sur HDFS en csv dans le dossier /refined/departement/v3/csv
    Cities.write_csv(cities_clean6,'/refined/departement/v3/csv','unique')

    # Stopper la spark session
    spark.stop()
