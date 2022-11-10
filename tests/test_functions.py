from spark_cities.functions import split_lat_long, departement, departement_udf
from spark_cities.models.cities import Cities

def test_split_lat_long(spark_session):
   # GIVEN
   # création d'un dataframe de test en utilisant la spark_session
   lst_gps=[('1.1,43.0','dummy'),('-0.4,-45.0','dummy')]
   df_input = spark_session.createDataFrame(lst_gps, [Cities.COORDONNEES_GPS,'dummy'])

   # WHEN
   df_result = split_lat_long(df_input)
   # transformer df_result en une list de dictionaire
   result = list(map(lambda x: x.asDict(), df_result.collect()))

   # THEN
   expected = [
     {Cities.LAT: 1.1, Cities.LON: 43.0, 'dummy':'dummy'},
     {Cities.LAT: -0.4, Cities.LON: -45.0, 'dummy':'dummy'}
   ]

   # vérifier l'égalité des deux listes
   assert result == expected

def test_departement(spark_session):
   # GIVEN
   # création d'un dataframe de test en utilisant la spark_session
   lst_code_postal=[('65234','65'),('76548','76'),('92009','92')]
   df_input = spark_session.createDataFrame(lst_code_postal, [Cities.CODE_POSTAL,Cities.DEPT])

   # WHEN
   df_result = df_input.withColumn(Cities.DEPT, departement(df_input[Cities.CODE_POSTAL]))
   # transformer df_result en une list de dictionaire
   result = list(map(lambda x: x.asDict(), df_result.collect()))

   # THEN
   expected = [
     {Cities.CODE_POSTAL: '65234', Cities.DEPT: '65'},
     {Cities.CODE_POSTAL: '76548', Cities.DEPT: '76'},
     {Cities.CODE_POSTAL: '92009', Cities.DEPT: '92'}
   ]

   # vérifier l'égalité des deux listes
   assert result == expected

def test_departement_udf(spark_session):
   # GIVEN
   # création d'un dataframe de test en utilisant la spark_session
   lst_code_postal=[('65234','65'),('20165','2A'),('20200','2B'),('20214','2B')]
   df_input = spark_session.createDataFrame(lst_code_postal, [Cities.CODE_POSTAL,Cities.DEPT])

   # WHEN
   df_result = df_input.withColumn(Cities.DEPT, departement_udf(df_input[Cities.CODE_POSTAL]))
   # transformer df_result en une list de dictionaire
   result = list(map(lambda x: x.asDict(), df_result.collect()))

   # THEN
   expected = [
     {Cities.CODE_POSTAL: '65234', Cities.DEPT: '65'},
     {Cities.CODE_POSTAL: '20165', Cities.DEPT: '2A'},
     {Cities.CODE_POSTAL: '20200', Cities.DEPT: '2B'},
     {Cities.CODE_POSTAL: '20214', Cities.DEPT: '2B'}
   ]

   # vérifier l'égalité des deux listes
   assert result == expected
   