#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  2 16:53:36 2020

@author: ionekr
"""

import logging
import urllib.parse
import urllib.request

from os import path
from zipfile import ZipFile


class covid:

    data_url = 'http://187.191.75.115/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip'
    zipfolder = '/home/ionekr/Documentos/Deformacion/Covid19/Datos'
    archivo = 'da_covid19.zip'

    def __init__(self, logger):
        self.logger = logger

    def descarga(self):
        try:
            self.logger.info("Inicia descarga de datos...")
            descarga = urllib.request.urlopen(self.data_url)
            with open(path.join(self.zipfolder, self.archivo), 'wb') as f:
                f.write(descarga.read())
            f.close()
            self.logger.info("Descargado")
            return True
        except IOError as e:
            self.logger.error(e)
            return False

    def extractor(self):
        try:
            self.logger.info("Descompriendo...")
            with ZipFile(path.join(self.zipfolder, self.archivo), 'r') as z:
                z.extractall(path=self.zipfolder)
            z.close()
            return True
        except IOError as e:
            self.logger.error("Problemas con zip", e)
            return False

    def preproceso(self):
        self.logger.info("Inicia proceso QD")
        if self.descarga():
            if self.extractor():
                self.logger.info("Podemos continuar...")
            else:
                self.logger.error("Sin acceso a archivo")
        else:
            self.logger.error("Problemas en descarga")


if __name__ == '__main__':
    logger = logging.getLogger("principal")
    logger.setLevel(logging.INFO)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    logger.info("Imprime?")
    covid19 = covid(logger)
    covid19.preproceso()
