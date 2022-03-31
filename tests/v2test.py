import unittest
from etl.jobs.v2 import demoV2

class SimpleTest(unittest.TestCase):
    def comprobarEscritura(self, outputData):
        assertIsNotNone(outputData, 'No se ha escrito nada')
    def comprobarLectura(self, inputData):
        assertIsNotNone(inputData, 'No encuentro archivo del que extraer los datos')


