1/ Pré-requis
	* Avoir hdfs qui tourne qui tourne
	* Installer si besoin les librairies suivantes :
		- pyspark (en 3.3.0)
		- math
		- numpy

2/ Exécuter le .egg en local
	spark-submit --master local --py-files /home/simplon/spark_cities/dist/SparkCities-0.1-py3.7.egg __main__.py
