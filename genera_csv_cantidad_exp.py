# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 20:18:22 2025

@author: flipe
"""

import os
import pandas as pd
import numpy as np
#pip install pandas-stubs
#pip install pyarrow fastparquet
folder_path_celonis = 'data/celonis'

#############
## FUNCIONES
#############


def cargar_expedientes(folder_path: str, filename: str = 'TramitaGlobal-GB_CASE_EXPEDIENTES.parquet') -> pd.DataFrame:
    """
    Carga un archivo Parquet de expedientes, selecciona ciertas columnas y las renombra.

    Parámetros:
    - folder_path (str): Ruta donde se encuentra el archivo Parquet.
    - filename (str): Nombre del archivo Parquet (por defecto 'TramitaGlobal-GB_CASE_EXPEDIENTES.parquet').

    Retorna:
    - pd.DataFrame: DataFrame con las columnas seleccionadas y renombradas.
    """
    
    columns_to_select = ['_CASE_KEY', 'NPROCADMI', 'NPROVINCI', 'NMUNICIPI', 'NORGANO', 
                         'NIDPF', 'NIDPJ', 'FECHALTA', 'FREGISTRO']

    file_path = os.path.join(folder_path, filename)

    # Cargar el DataFrame solo con las columnas necesarias
    df = pd.read_parquet(file_path, columns=columns_to_select)

    # Renombrar columnas
    df.rename(columns={'_CASE_KEY': 'id_exp',
                       'NPROCADMI': 'cod_procedimiento',
                       'NPROVINCI': 'cod_provincia',
                       'NMUNICIPI': 'cod_municipio',
                       'NORGANO': 'cod_organo',
                       'NIDPF': 'dni',
                       'NIDPJ': 'nif',
                       'FECHALTA': 'fecha_alta_exp',
                       'FREGISTRO': 'fecha_registro_exp'}, inplace=True)
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

    return df

def cargar_procedimientos(folder_path: str, filename: str = 'TramitaGlobal-GB_PROCEDIMIENTOS.parquet') -> pd.DataFrame:
    """
    Carga un archivo Parquet de procedimientos, selecciona ciertas columnas y las renombra.

    Parámetros:
    - folder_path (str): Ruta donde se encuentra el archivo Parquet.
    - filename (str): Nombre del archivo Parquet (por defecto 'TramitaGlobal-GB_PROCEDIMIENTOS.parquet').

    Retorna:
    - pd.DataFrame: DataFrame con las columnas seleccionadas y renombradas.
    """
    
    columns_to_select = ['NPROCADMI', 'CDENOMINA', 'CDESCRIPC', 'NORGCOMP', 
                         'CSIACI', 'CONSEJERIA', 'ORGANO_INSTRUCTOR']

    file_path = os.path.join(folder_path, filename)

    # Cargar el DataFrame solo con las columnas necesarias
    df = pd.read_parquet(file_path, columns=columns_to_select)

    # Renombrar columnas
    df.rename(columns={'NPROCADMI': 'codigo_procedimiento',
                       'CDENOMINA': 'denominacion',
                       'CDESCRIPC': 'descripcion',
                       'NORGCOMP': 'cod_organo_competente_proc',
                       'CSIACI': 'siaci',
                       'CONSEJERIA': 'consejeria',
                       'ORGANO_INSTRUCTOR': 'org_instructor'}, inplace=True)
    
    df['codigo_procedimiento'] = df['codigo_procedimiento'].astype('uint32')


    df['codigo_procedimiento'] = df['codigo_procedimiento'].astype('uint16')
    df['cod_organo_competente_proc'] = df['cod_organo_competente_proc'].astype('uint16')
    df['denominacion'] = df['denominacion'].astype("string")
    df['descripcion'] = df['descripcion'].astype("string")
    df['siaci'] = df['siaci'].astype("string")
    df['consejeria'] = df['consejeria'].astype("string")
    df['org_instructor'] = df['org_instructor'].astype("string")

    return df



df_expedientes = cargar_expedientes(folder_path_celonis)
df_procedimientos = cargar_procedimientos(folder_path_celonis)


# Realizamos una fusión (merge) entre los DataFrames df_expedientes y df_procedimientos.
# Se usa 'cod_procedimiento' en df_expedientes y 'codigo_procedimiento' en df_procedimientos
# como claves para la unión. La fusión es 'left', lo que significa que se mantendrán 
# todas las filas de df_expedientes, incluso si no hay coincidencias en df_procedimientos.

df_exp_proc = pd.merge(df_expedientes, df_procedimientos,
                left_on = 'cod_procedimiento',
                right_on = 'codigo_procedimiento',
                how = 'left')

# Seleccionamos únicamente las columnas relevantes para reducir el tamaño del DataFrame.
df_exp_proc = df_exp_proc[['consejeria', 'org_instructor',
                           'cod_procedimiento', 'denominacion', 'descripcion', 'siaci',
                           'id_exp']]


df_grouped = df_exp_proc.groupby('cod_procedimiento', as_index=False).agg(
    consejeria=('consejeria', 'first'),
    org_instructor=('org_instructor', 'first'),
    denominacion=('denominacion', 'first'),
    descripcion=('descripcion', 'first'),
    siaci=('siaci', 'first'),
    numero_expedientes=('id_exp', 'count'),
)



df_grouped = df_grouped.sort_values(
    by=['consejeria', 'org_instructor', 'numero_expedientes'],
    ascending=[True, True, False]  # Ascending for consejeria & org_instructor, descending for numero_expedientes
)
     

folder_path_apoyo = 'data/apoyo'

df_grouped.to_csv(os.path.join(folder_path_apoyo, 'expedientes_x_procedimiento.csv'), index=False, sep =';', encoding = 'utf-8-sig')
     

