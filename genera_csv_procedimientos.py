# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 20:31:26 2025

@author: flipe
"""

import os
import pandas as pd


folder_path = 'data/celonis'
file_path_procedimientos_raw = os.path.join(folder_path, 'TramitaGlobal-GB_PROCEDIMIENTOS.parquet')


df_procedimientos_raw = pd.read_parquet(file_path_procedimientos_raw)


df = df_procedimientos_raw[['CONSEJERIA',
                            'ORGANO_INSTRUCTOR',
                            'NPROCADMI',
                            'CSIACI',
                            'CDENOMINA',
                            'CDESCRIPC']].rename(columns={
                                'NPROCADMI': 'codigo_procedimiento',
                                'CSIACI': 'siaci',
                                'CONSEJERIA': 'consejeria',
                                'ORGANO_INSTRUCTOR': 'org_instructor',
                                'CDENOMINA': 'denominacion',
                                'CDESCRIPC': 'descripcion',})
                                
                                
df_sorted = df.sort_values(by=['consejeria', 'org_instructor', 'codigo_procedimiento'], ascending=[True, False, True])

df_sorted['codigo_procedimiento'] = df_sorted['codigo_procedimiento'].astype(int)


folder_path_apoyo = 'data/apoyo'
file_path_csv_codigos = os.path.join(folder_path_apoyo, 'codigos_procedimientos.csv')

# Exportar a CSV con BOM para evitar problemas de codificación en Excel
df_sorted.to_csv(file_path_csv_codigos, index=False, encoding='utf-8-sig', sep = ';')



