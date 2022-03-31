from configparser import ConfigParser
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def execute():
#    file = 'ReadConfig.py'
#    config = configparser.ConfigParser()
#    config.read(file)
#    inputData = config.get("inputData")
#    outputData = config.get("outputData") 

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