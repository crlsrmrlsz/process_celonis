# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 20:35:58 2025

@author: flipe
"""

import os
import pandas as pd
import numpy as np


# Define the folder where all CSV files are located
folder_path_celonis = 'data/celonis'
folder_path_apoyo = 'data/apoyo'
folder_path_tratados = 'data/tratados'


###################
## FUNCIONES
###################

def cargar_expedientes(folder_path: str, filename: str = 'TramitaGlobal-GB_CASE_EXPEDIENTES.parquet') -> pd.DataFrame:
    """
    Carga un archivo Parquet con información de expedientes administrativos, 
    seleccionando solo ciertas columnas para optimizar el uso de memoria y 
    renombrando las columnas para mayor claridad.

    Parámetros:
    - folder_path (str): Ruta de la carpeta donde se encuentra el archivo.
    - filename (str): Nombre del archivo Parquet a leer (por defecto: 'TramitaGlobal-GB_CASE_EXPEDIENTES.parquet').

    Retorna:
    - pd.DataFrame: DataFrame con los datos de expedientes procesados.
    """

    # Columnas seleccionadas para reducir el tamaño del DataFrame
    columns_to_select = ['_CASE_KEY', 'NPROCADMI', 'NPROVINCI', 'NMUNICIPI', 'NORGANO', 
                         'NIDPF', 'NIDPJ', 'FECHALTA', 'FREGISTRO']

    # Ruta completa del archivo
    file_path = os.path.join(folder_path, filename)
    # Cargar el archivo Parquet con las columnas seleccionadas
    df = pd.read_parquet(file_path, columns=columns_to_select)

    # Renombrar columnas para mayor claridad
    rename_dict = {
        '_CASE_KEY': 'id_exp',
        'NPROCADMI': 'cod_procedimiento',
        'NPROVINCI': 'cod_provincia',
        'NMUNICIPI': 'cod_municipio',
        'NORGANO': 'cod_organo',
        'NIDPF': 'dni',
        'NIDPJ': 'nif',
        'FECHALTA': 'fecha_alta_exp',
        'FREGISTRO': 'fecha_registro_exp'
    }
    df.rename(columns=rename_dict, inplace=True)
    
    
    df['id_exp'] = df['id_exp'].astype('uint32')
    df['cod_procedimiento'] = df['cod_procedimiento'].astype('uint32')
    df['cod_provincia'] = df['cod_provincia'].astype(pd.Int16Dtype())
    df['cod_municipio'] = df['cod_municipio'].astype(pd.Int16Dtype())
    df['cod_organo'] = df['cod_organo'].astype(pd.Int16Dtype())
    df['dni'] = df['dni'].astype(pd.Int32Dtype())
    df['nif'] = df['nif'].astype(pd.Int32Dtype())
    
    #to create a 5 digits ine code for town: 2 digits province + 3 digits town
    df['codine_provincia'] = df['cod_provincia'].astype('string').str.zfill(2)
    df['codine_municipio'] = df['cod_municipio'].astype('string').str.zfill(3)
    # Concatenate formatted codes
    df['codine'] = df['codine_provincia'] + df['codine_municipio']
    
        
    df = df[['id_exp', 'cod_procedimiento', 'cod_provincia', 'cod_organo', 'dni', 'nif', 'fecha_alta_exp', 'fecha_registro_exp',
           'codine_provincia', 'codine', 'cod_municipio']]

    return df



def cargar_procedimientos(folder_path: str, filename: str = 'TramitaGlobal-GB_PROCEDIMIENTOS.parquet') -> pd.DataFrame:
    """
    Carga un archivo Parquet con información de procedimientos administrativos,
    seleccionando solo ciertas columnas para optimizar el uso de memoria y renombrando columnas para mayor claridad.

    Parámetros:
    - folder_path (str): Ruta de la carpeta donde se encuentra el archivo.
    - filename (str): Nombre del archivo Parquet a leer (por defecto: 'TramitaGlobal-GB_PROCEDIMIENTOS.parquet').

    Retorna:
    - pd.DataFrame: DataFrame con los datos de procedimientos procesados.
    """

    # Columnas seleccionadas para reducir el tamaño del DataFrame
    columns_to_select = ['NPROCADMI', 'CDENOMINA', 'CDESCRIPC', 
                         'CSIACI', 'CONSEJERIA', 'ORGANO_INSTRUCTOR']

    # Ruta completa del archivo
    file_path = os.path.join(folder_path, filename)

    # Cargar el archivo Parquet con las columnas seleccionadas
    df_procedimientos = pd.read_parquet(file_path, columns=columns_to_select)

    # Renombrar columnas para mayor claridad
    df_procedimientos.rename(columns={
        'NPROCADMI': 'codigo_procedimiento',
        'CSIACI': 'siaci',
        'CONSEJERIA': 'consejeria',
        'ORGANO_INSTRUCTOR': 'org_instructor',
        'CDENOMINA': 'denominacion',
        'CDESCRIPC': 'descripcion'
    }, inplace=True)

    # Convertir tipos de datos para optimizar memoria
    df_procedimientos['codigo_procedimiento'] = df_procedimientos['codigo_procedimiento'].astype('uint16')
    df_procedimientos['denominacion'] = df_procedimientos['denominacion'].astype("string")
    df_procedimientos['descripcion'] = df_procedimientos['descripcion'].astype("string")
    df_procedimientos['siaci'] = df_procedimientos['siaci'].astype("string")
    df_procedimientos['consejeria'] = df_procedimientos['consejeria'].astype("string")
    df_procedimientos['org_instructor'] = df_procedimientos['org_instructor'].astype("string")

    # Seleccionar solo las columnas de interés
    df_procedimientos = df_procedimientos[['codigo_procedimiento', 'denominacion', 'descripcion',
                                           'consejeria', 'org_instructor']]
    
    return df_procedimientos


def cargar_municipios(folder_path: str) -> pd.DataFrame:
    """
    Carga y procesa el archivo 'cod_municipio_es.csv' ubicado en la carpeta especificada.
    
    Parámetros:
        folder_path (str): Ruta de la carpeta donde se encuentra el archivo CSV.
    
    Retorna:
        pd.DataFrame: DataFrame con los municipios procesados.
    """
    file_path = os.path.join(folder_path, 'cod_municipio_es.csv')
    df = pd.read_csv(file_path, sep=';', encoding='utf-8')  
    df['municipio'] = df['municipio'].astype("string")
    df['cod_provincia'] = df['cod_provincia'].astype(pd.Int16Dtype())
    df['cod_municipio'] = df['cod_municipio'].astype(pd.Int16Dtype())
    
    return df


def cargar_provincias(folder_path: str) -> pd.DataFrame:
    """
    Carga y procesa el archivo 'cod_provincia_es.csv' ubicado en la carpeta especificada.
    
    Parámetros:
        folder_path (str): Ruta de la carpeta donde se encuentra el archivo CSV.
    
    Retorna:
        pd.DataFrame: DataFrame con las provincias procesadas.
    """
    file_path = os.path.join(folder_path, 'cod_provincia_es.csv')
    df = pd.read_csv(file_path, sep=';', encoding='utf-8')  
    df['provincia'] = df['provincia'].astype("string")
    df['cod_provincia'] = df['cod_provincia'].astype(pd.Int16Dtype())
    
    return df


def cargar_unidad_tramitadora(folder_path: str) -> pd.DataFrame:
    """
    Carga y procesa el archivo 'TramitaGlobal-GB_UNIDAD_TRAMITADORA.parquet' ubicado en la carpeta especificada.
    
    Parámetros:
        folder_path (str): Ruta de la carpeta donde se encuentra el archivo Parquet.
    
    Retorna:
        pd.DataFrame: DataFrame con las unidades tramitadoras procesadas.
    """
    columns_to_filter = ['CODORG', 'DESCRIPCION']
    file_path = os.path.join(folder_path, 'TramitaGlobal-GB_UNIDAD_TRAMITADORA.parquet')
    df = pd.read_parquet(file_path, columns=columns_to_filter)
    df.rename(columns={'CODORG': 'cod_organo', 'DESCRIPCION': 'unidad_tramitadora'}, inplace=True)
    df['unidad_tramitadora'] = df['unidad_tramitadora'].astype("string")
    df['cod_organo'] = df['cod_organo'].astype('uint16')
    
    return df

def cargar_solicitudes(folder_path: str) -> pd.DataFrame:
    """
    Carga y procesa el archivo 'TramitaGlobal-VW_SOLICITUDES_VENTANILLA.parquet' ubicado en la carpeta especificada.
    
    Parámetros:
        folder_path (str): Ruta de la carpeta donde se encuentra el archivo Parquet.
    
    Retorna:
        pd.DataFrame: DataFrame con las solicitudes procesadas.
    """
    columns_to_filter = ['NEXPEDTRAM', 'CMODOPRE']
    file_path = os.path.join(folder_path, 'TramitaGlobal-VW_SOLICITUDES_VENTANILLA.parquet')
    df = pd.read_parquet(file_path, columns=columns_to_filter)
    df.rename(columns={'NEXPEDTRAM': 'cod_expediente', 'CMODOPRE': 'forma_presentacion'}, inplace=True)
    
    def es_telematica(value):
        return value not in ['P']
    
    df['es_telematica'] = df['forma_presentacion'].apply(es_telematica)
    df.drop(columns=['forma_presentacion'], inplace=True)
    df.dropna(subset=['cod_expediente'], inplace=True)
    df['cod_expediente'] = df['cod_expediente'].astype('uint32')
    
    return df

def cargar_tramites(folder_path: str) -> pd.DataFrame:
    """
    Carga y procesa el archivo 'TramitaGlobal-GB_ACTIVITIES_TRAMITES.parquet' ubicado en la carpeta especificada.
    
    Parámetros:
        folder_path (str): Ruta de la carpeta donde se encuentra el archivo Parquet.
    
    Retorna:
        pd.DataFrame: DataFrame con los trámites procesados.
    """
    columns_to_filter = ['_ACTIVITY_ES', '_EVENTIME', 'NTRAMITE', 'NPROCADMI', '_SORTING', 'FECHA_ANULACION_TRAMITE', '_CASE_ID']
    file_path = os.path.join(folder_path, 'TramitaGlobal-GB_ACTIVITIES_TRAMITES.parquet')
    df = pd.read_parquet(file_path, columns=columns_to_filter)
    df.rename(columns={'_ACTIVITY_ES': 'desc_tramite', '_EVENTIME': 'fecha_tramite', 'FECHA_ANULACION_TRAMITE': 'fecha_anulacion', 'NTRAMITE': 'num_tramite', 'NPROCADMI': 'cod_procedimiento', '_SORTING': 'orden_tramite', '_CASE_ID': 'cod_expediente'}, inplace=True)
    df['desc_tramite'] = df['desc_tramite'].astype("string")
    df['orden_tramite'] = df['orden_tramite'].astype('uint16')
    df['cod_expediente'] = df['cod_expediente'].astype('uint32')
    df['num_tramite'] = df['num_tramite'].astype('uint16')
    
    # Eliminar filas donde 'fecha_anulacion' no sea nula
    df = df[df['fecha_anulacion'].isnull()]
    df.drop(columns=['fecha_anulacion'], inplace=True)
    
    return df


def generar_expedientes_completo(folder_path: str, df_expedientes: pd.DataFrame, df_procedimientos: pd.DataFrame, df_municipios: pd.DataFrame, df_provincias: pd.DataFrame, df_unidad_tramitadora: pd.DataFrame, df_solicitudes: pd.DataFrame) -> pd.DataFrame:
    """Realiza la fusión de varios DataFrames y guarda el resultado en un archivo parquet."""
    df_exp = pd.merge(df_expedientes, df_municipios, on=['cod_municipio', 'cod_provincia'], how='left')
    df_exp = pd.merge(df_exp, df_provincias, on='cod_provincia', how='left').drop(columns=['cod_municipio', 'cod_provincia'])
    df_exp = pd.merge(df_exp, df_unidad_tramitadora, on='cod_organo', how='left').drop(columns=['cod_organo'])
    df_exp = pd.merge(df_exp, df_procedimientos, left_on='cod_procedimiento', right_on='codigo_procedimiento', how='inner').drop(columns=['cod_procedimiento'])
    df_exp = pd.merge(df_exp, df_solicitudes, left_on='id_exp', right_on='cod_expediente', how='left').drop(columns=['cod_expediente'])
    df_exp.to_parquet(os.path.join(folder_path, 'expedientes_completo.parquet'))
    return df_exp


def generar_tramites_completo(df_exp: pd.DataFrame, df_tramites: pd.DataFrame, folder_path: str):
    """
    Merges two dataframes on specific columns, drops unnecessary columns, 
    and saves the result as a Parquet file.

    Parameters:
    df_exp (pd.DataFrame): Main dataframe containing expedientes.
    df_tramites (pd.DataFrame): Dataframe containing tramites.
    folder_path (str): Path to save the resulting Parquet file.
    output_filename (str, optional): Name of the output Parquet file. Default is 'tramites_completo.parquet'.

    Returns:
    pd.DataFrame: The merged and cleaned dataframe.
    """
    df_merged = pd.merge(df_exp, df_tramites,
                          left_on='id_exp',
                          right_on='cod_expediente',
                          how='inner')
    
    columns_to_drop = ['cod_expediente', 'fecha_alta_exp', 'fecha_registro_exp']
    df_merged = df_merged.drop(columns=columns_to_drop, axis=1)
    
    output_path = os.path.join(folder_path, 'tramites_completo.parquet')
    print(f"df_tramites_completo , len : {len(df_merged)}" )
    df_merged.to_parquet(output_path)
    



df_expedientes = cargar_expedientes(folder_path_celonis)
print(f"df_expedientes , len : {len(df_expedientes)}" )
df_procedimientos = cargar_procedimientos(folder_path_celonis)
df_municipios = cargar_municipios(folder_path_apoyo)
df_provincias = cargar_provincias(folder_path_apoyo)
df_unidad_tramitadora = cargar_unidad_tramitadora(folder_path_celonis)
df_solicitudes = cargar_solicitudes(folder_path_celonis)
print(f"df_solicitudes , len : {len(df_solicitudes)}" )
df_tramites = cargar_tramites(folder_path_celonis)
print(f"df_tramites , len : {len(df_tramites)}" )

df_expedientes_completo = generar_expedientes_completo(folder_path_tratados,
                             df_expedientes,
                             df_procedimientos,
                             df_municipios, 
                             df_provincias, 
                             df_unidad_tramitadora, 
                             df_solicitudes)
print(f"df_expedientes_completo , len : {len(df_expedientes_completo)}" )
generar_tramites_completo(df_expedientes_completo,
                          df_tramites,
                          folder_path_tratados)

