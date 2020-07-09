# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 19:28:32 2020

@author: Luis Omar
"""


import logging
import pandas as pd
import re
import os

class Preprocesador:
    
    LLAVE = ['ID_REGISTRO', 'ORIGEN', 'SECTOR', 'ENTIDAD_UM', 'SEXO',
             'ENTIDAD_NAC', 'ENTIDAD_RES', 'MUNICIPIO_RES', 'TIPO_PACIENTE',
             'FECHA_INGRESO', 'FECHA_SINTOMAS', 'FECHA_DEF', 'INTUBADO',
             'NEUMONIA', 'EDAD', 'NACIONALIDAD', 'EMBARAZO',
             'HABLA_LENGUA_INDIG', 'DIABETES', 'EPOC', 'ASMA', 'INMUSUPR',
             'HIPERTENSION', 'OTRA_COM', 'CARDIOVASCULAR', 'OBESIDAD',
             'RENAL_CRONICA', 'TABAQUISMO', 'OTRO_CASO', 'RESULTADO',
             'MIGRANTE', 'PAIS_NACIONALIDAD', 'PAIS_ORIGEN', 'UCI']
    LLAVE_BK = ['ID_REGISTRO', 'FECHA_ACTUALIZACION', 'RESULTADO', 'FECHA_DEF',
                'TIPO_PACIENTE', 'INTUBADO', 'UCI']
    LLAVE_RP = ['FECHA_ACTUALIZACION', 'RESULTADO', 'FECHA_DEF',
                'TIPO_PACIENTE', 'INTUBADO', 'UCI']
    NOMBRES_BK = ['FECHA_ACTUALIZACION_PREV', 'RESULTADO_PREV',
                  'FECHA_DEF_PREV', 'TIPO_PACIENTE_PREV',
                  'INTUBADO_PREV', 'UCI_PREV']
    SET_KF = ['FECHA_ACTUALIZACION_PREV', 'FECHA_DEF_PREV']
    KEYS = ['ID_REGISTRO', 'TIPO_PACIENTE', 'INTUBADO', 'RESULTADO','UCI']
    KEY_FECHAS = ['FECHA_ACTUALIZACION', 'FECHA_INGRESO', 'FECHA_SINTOMAS',
                  'FECHA_DEF']
    
    def __init__(self, logger, dias_activo, ruta_entrada, ruta_salida,
                 es_base):
        self.logger = logger
        self.dias_activo = dias_activo
        self.ruta_entrada = ruta_entrada
        self.ruta_salida = ruta_salida
        self.es_base = es_base

    def es_activo(self, dias):
        if dias <= self.dias_activo:
            return 1
        else:
            return 0
    
    def lista_dir(self):
        patron = re.compile('\d{6}COVID19MEXICO.*\.csv')
        archivos = [os.path.join(r, f1) for r,d,f in 
                    os.walk(self.ruta_entrada) for f1 in f if patron.match(f1)]
        return archivos
    
    def key_difer(self, df, dfbase):
        """Determina los ID_REGISTRO omitidos o eliminados en la nueva version"""
        self.logger.info("Buscando ID_REGISTROs comunes")
        self.logger.info("Dimensiones entrada (%d, %d)" % df.shape)
        df_idx = df.merge(dfbase, how='outer', on='ID_REGISTRO',
                          suffixes=('_x', '_y'), indicator=True)
        nx = df_idx[df_idx._merge=='right_only'].ID_REGISTRO.values.tolist()
        self.logger.warning("Núm. registros perdidos: %d" % len(nx))
        nw = df_idx[df_idx._merge=='left_only'].ID_REGISTRO.values.tolist()
        self.logger.warning("Núm. registros añadidos: %d" % len(nw))
        ni = df_idx[df_idx._merge=='both'].ID_REGISTRO.values.tolist()
        self.logger.warning("Núm. registros consistentes: %d" % len(ni))
        self.logger.info("Total registros: %d" % (len(nx) + len(nw) + len(ni)))
        del df_idx
        return nx, nw, ni
    
    def difer_reg(self, df, dfbase):
        df_act = df[self.KEYS]
        df_prev = dfbase[self.KEYS]
        df_prev.columns = df_act.columns
        idreg_dif = (df_act != df_prev).any(1)
        del df_act, df_prev
        self.logger.info("Núm. regs con diferencia: %d" % idreg_dif.shape[0])
        self.logger.info("tipo: %s" % type(idreg_dif))
        return idreg_dif
        
    
    def procesa_dfs(self, df, dfbase):
        iant, invs, iin = self.key_difer(df[['ID_REGISTRO']],
                                    dfbase[['ID_REGISTRO']])
        for c in self.KEY_FECHAS:
            df[c] = pd.to_datetime(df[c], format='%Y-%m-%d', errors='coerce')
        df['DIAS_FIS'] = (df.FECHA_ACTUALIZACION - df.FECHA_SINTOMAS).dt.days
        df['ACTIVO'] = df.DIAS_FIS.apply(self.es_activo)
        df_comun = df[df.ID_REGISTRO.isin(iin)].reset_index()
        dfbase_comun = dfbase[dfbase.ID_REGISTRO.isin(iin)].reset_index()
        df_comun = df_comun.reset_index(drop=True)
        dfbase_comun = dfbase_comun.reset_index(drop=True)
        reg_dif = self.difer_reg(df_comun, dfbase_comun)
        lst_sin_cambio = dfbase_comun[~reg_dif].ID_REGISTRO.values.tolist()
        iant.extend(lst_sin_cambio)        
        dfpr = dfbase[dfbase.ID_REGISTRO.isin(iant)]  # perdidos + sin cambios
        for c in self.SET_KF:
            dfpr[c] = pd.to_datetime(dfpr[c], format='%Y-%m-%d',
                                     errors='coerce')
        # respaldando cambios
        dfcmb = df_comun[reg_dif].merge(dfbase_comun[reg_dif][self.LLAVE_BK],
                                        how='inner', on='ID_REGISTRO',
                                        suffixes=('', '_PREV'))
        for c in self.SET_KF:
            dfcmb[c] = pd.to_datetime(dfcmb[c], format='%Y-%m-%d',
                                      errors='coerce')
        # nuevos
        dfn = df[df.ID_REGISTRO.isin(invs)]
        ract = df[df.ID_REGISTRO.isin(invs)][self.LLAVE_RP]
        ract.columns = self.NOMBRES_BK    
        df_news = pd.concat([dfn, ract], axis=1)  # todos tienen las mismas cols
        for c in self.SET_KF:
            df_news[c] = pd.to_datetime(df_news[c], format='%Y-%m-%d',
                                        errors='coerce')        
        df_all = pd.concat([dfpr, dfcmb])
        df_all = pd.concat([df_all, df_news])
        self.logger.info("Núm registros: %d" % df_all.shape[0])
        # set formato fechas
        for c in self.KEY_FECHAS:
            df_all[c] = pd.to_datetime(df_all[c], format='%Y-%m-%d',
                                       errors='coerce')

        # eliminar
        df_all['DIAS_FIS'] = (df.FECHA_ACTUALIZACION - df.FECHA_SINTOMAS).dt.days
        df_all['ACTIVO'] = df.DIAS_FIS.apply(self.es_activo)
        df_all.to_csv(self.ruta_salida, index=False, encoding='latin1')
        
        
    def base(self, archivo):
        try:
            self.logger.info("Procesando archivo: [%s]" % archivo)
            df = pd.read_csv(archivo, encoding='utf8')
            for llv in self.KEY_FECHAS:
                df[llv] = pd.to_datetime(df[llv], format='%Y-%m-%d',
                                         errors='coerce')
            df['DIAS_FIS'] = (df.FECHA_ACTUALIZACION - df.FECHA_SINTOMAS). \
                dt.days
            df['ACTIVO'] = df.DIAS_FIS.apply(self.es_activo)
            for k1, k2 in zip(self.NOMBRES_BK, self.LLAVE_RP):
                df[k1] = df[k2]
            return df
        except IOError as e:
            self.logger.error("Error [%s]" % e)
            return None

    def revisor(self, dfbase, archivo):
        try:
            try:
                df_act = pd.read_csv(archivo, encoding='utf8')
            except UnicodeDecodeError as e:
                self.logger.error(e)
                self.logger.warning("Ahora, probamos con latin1")
                df_act = pd.read_csv(archivo, encoding='latin1')                
            self.procesa_dfs(df_act, dfbase)
        except IOError as e:
            self.logger.error("Error [%s]" % e)
            return None

    def appender(self, lista_archivos):
        try:
            for archivo in lista_archivos:
                self.logger.info("Integrando archivo: [%s]" % archivo)
                dfbase = pd.read_csv(self.ruta_salida, encoding='latin1')
                self.revisor(dfbase, archivo)
            self.logger.info("Archivos procesados %d" % len(lista_archivos))
        except IOError as e:
            self.logger.error("Problemas con: " + e)
        

    def procesa(self):
        self.logger.info("Inicia el preproceso de info COVID19")
        archivos = self.lista_dir()
        if self.es_base:
            df = self.base(archivos[0])
            print(df.head())
            df.to_csv(self.ruta_salida, encoding='latin1', index=False)
        else:
            self.appender(archivos[1:])
            self.logger.warning("Metodos por desarrollar")
            
if __name__ == '__main__':
    logger = logging.getLogger("principal.log")
    logger.setLevel(logging.INFO)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    logger.info("Tres meses a procesar: abril, mayo y junio")
    ruta_entrada = r'C:\Users\Luis Omar\Documents\Datos\covid\unzip'
    ruta_salida = r'C:\Users\Luis Omar\Documents\Datos\covid\consol\consol_new.csv'
    preprocesador = Preprocesador(logger, 14, ruta_entrada, ruta_salida, False)
    preprocesador.procesa()
        
