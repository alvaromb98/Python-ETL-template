from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

def execute():

    inputData=sys.argv[1]
    outputData=sys.argv[2]

    spark = SparkSession.builder.getOrCreate()

    # Entrada de los datos
    job = spark.read.csv(inputData, sep=',', header=True)

    # Transform

    # Salida de los datos
    job.write.mode('overwrite').option('header', True).csv(outputData)


if __name__ == '__main__':
    execute()
