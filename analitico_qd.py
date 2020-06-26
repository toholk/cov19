#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  2 17:42:32 2020

@author: ionekr
"""

import logging
import pandas as pd


class anlt_dq:

    df_prv = None
    df_act = None
    act_prev_state = None
    act_prev_key_state = None
    cols_omit = ['FECHA_ACTUALIZACION', 'RESULTADO', 'ID_REGISTRO',
                 'FECHA_DEF']

    def __init__(self, logger, csv1, csv2):
        self.logger = logger
        try:
            self.df_prv = pd.read_csv(csv1, encoding='latin1')
            self.df_act = pd.read_csv(csv2, encoding='latin1')
            self.df_prv['MUNICIPIO_RES'] = self.df_prv.MUNICIPIO_RES. \
                astype(str).replace('.0', '')
            self.df_prv['MUNICIPIO_RES'] = self.df_prv.MUNICIPIO_RES. \
                str.replace('.0', '')
            self.df_act['MUNICIPIO_RES'] = self.df_prv.MUNICIPIO_RES. \
                astype(str).replace('.0', '')
            self.df_act['MUNICIPIO_RES'] = self.df_prv.MUNICIPIO_RES. \
                str.replace('.0', '')
            self.cols = self.df_act.columns
            self.cols_concat = [x for x in self.cols if x not in
                                self.cols_omit]
        except IOError as e:
            self.logger.error("NO HAY LECTURA DE DATOS: ", e)

    def gen_llaves(self):
        self.logger.info("Generando llaves")
        self.df_prv['LLAVE'] = self.df_prv[self.cols_concat].\
            apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
        self.df_act['LLAVE'] = self.df_act[self.cols_concat].\
            apply(lambda row: '-'.join(row.values.astype(str)), axis=1)
        self.act_prev_state = (self.df_act[['ID_REGISTRO','LLAVE', 'RESULTADO']]).\
            merge((self.df_prv[['LLAVE', 'RESULTADO']]),
                  on='LLAVE', how='left')
        self.act_prev_key_state = (self.df_act[['ID_REGISTRO','LLAVE', 'RESULTADO']]).\
            merge((self.df_prv[['ID_REGISTRO', 'RESULTADO']]),
                  on='ID_REGISTRO', how='left')
        self.act_prev_key_state.fillna(0, inplace=True)
        self.act_prev_state.fillna(0, inplace=True)


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
    ruta1 = '/home/ionekr/Documentos/Deformacion/Covid19/Datos/200508COVID19MEXICO.csv'
    ruta2 = '/home/ionekr/Documentos/Deformacion/Covid19/Datos/200509COVID19MEXICO.csv'    
    dq = anlt_dq(logger, ruta1, ruta2)
    dq.gen_llaves()
