from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from configparser import ConfigParser
import sys

def execute():

    if len(sys.argv) >= 2:
        inputData = sys.argv[1]
        outputData = sys.argv[2]
        
        proccessData(inputData, outputData)

    else:
        print("No ha introducido las rutas requeridas, se accederá al fichero external.config")
        filename = "external.config"
        contents = open(filename).read()
        config = eval(contents)
        inputData = config('inputData')
        outputData = config('outputData') 

        proccessData(inputData, outputData)


if __name__ == '__main__':
    execute()


def proccessData(inputData, outputData):
    spark = SparkSession.builder.getOrCreate()

    # Entrada de los datos
    job = spark.read.csv(inputData, sep=',', header=True)

    # Salida de los datos
    job.write.mode('overwrite').option('header', True).csv(outputData)