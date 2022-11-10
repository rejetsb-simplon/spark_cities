from setuptools import setup, find_packages
setup(
  name="SparkCities",
  version="0.1",
  packages=find_packages(include=['spark_cities', 'spark_cities.*']),

  # Project uses reStructuredText, so ensure that the docutils get
  # installed or upgraded on the target machine
  install_requires=["pyspark==3.3.0"],

  # metadata to display on PyPI
  author="me",
  author_email="me@example.com",
  description="First Application",
)
