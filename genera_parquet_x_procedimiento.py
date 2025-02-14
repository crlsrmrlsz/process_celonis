# -*- coding: utf-8 -*-
"""
Created on Sat Feb  1 19:33:35 2025

@author: flipe
"""


import os
import pandas as pd


folder_path_celonis = 'data/celonis'
folder_path_apoyo = 'data/apoyo'
folder_path_tratados = 'data/tratados'

columna_proc = 'codigo_procedimiento'

file_path_expedientes = os.path.join(folder_path_tratados, 'expedientes_completo.parquet')
file_path_tramites = os.path.join(folder_path_tratados, 'tramites_completo.parquet')


df_expedientes = pd.read_parquet(file_path_expedientes, engine='pyarrow')
df_tramites = pd.read_parquet(file_path_tramites, engine='pyarrow')

print(f"leidos ficheros completos, {len(df_expedientes)} expedientes y {len(df_tramites)} tramites")

# Group the dataframes by their respective filtering columns.
# This creates a dictionary where each key is a unique value and 
# the value is the corresponding sub-dataframe.
groups1 = dict(tuple(df_expedientes.groupby(columna_proc)))
groups2 = dict(tuple(df_tramites.groupby(columna_proc)))

print(f"grupo expedientes:  {len(groups1)} procedimientos distintos, grupo tramites {len(groups2)} procedimientos disintos")
# Get the union of the unique values from both dataframes.
all_values = set(groups1.keys()).union(groups2.keys())
print(f"total procedimientos diferentes : {len(all_values)}")
# Base output folder (change as needed)
output_base = folder_path_tratados

for value in all_values:
    # Create a folder for the current value. Using os.path.join ensures correct path formatting.
    folder = os.path.join(output_base, str(value))
    os.makedirs(folder, exist_ok=True)
    
    # Write the filtered data from df1, if it exists for this value.
    if value in groups1:
        output_path_df1 = os.path.join(folder, 'expedientes.parquet')
        groups1[value].to_parquet(output_path_df1, engine='pyarrow', index=False)
    
    # Write the filtered data from df2, if it exists for this value.
    if value in groups2:
        output_path_df2 = os.path.join(folder, 'tramites.parquet')
        groups2[value].to_parquet(output_path_df2, engine='pyarrow', index=False)
        
        
        
        
# # -*- coding: utf-8 -*-
# """
# Created on Sat Feb  1 19:33:35 2025

# @author: flipe
# """

# import os
# import pandas as pd

# folder_path_tratados = 'data/tratados'
# columna_proc = 'codigo_procedimiento'

# file_path_expedientes = os.path.join(folder_path_tratados, 'expedientes_completo.parquet')
# file_path_tramites = os.path.join(folder_path_tratados, 'tramites_completo.parquet')

# df_expedientes = pd.read_parquet(file_path_expedientes, engine='pyarrow')
# df_tramites = pd.read_parquet(file_path_tramites, engine='pyarrow')

# print(f"Leídos ficheros completos, {len(df_expedientes)} expedientes y {len(df_tramites)} trámites")

# groups_expedientes = dict(tuple(df_expedientes.groupby(columna_proc)))
# groups_tramites = dict(tuple(df_tramites.groupby(columna_proc)))

# all_procedures = set(groups_expedientes.keys()).union(groups_tramites.keys())
# print(f"Total procedimientos diferentes: {len(all_procedures)}")

# output_base = folder_path_tratados

# for procedure in all_procedures:
#     procedure_folder = os.path.join(output_base, str(procedure))
#     os.makedirs(procedure_folder, exist_ok=True)
    
#     if procedure in groups_expedientes:
#         expedientes_path = os.path.join(procedure_folder, 'expedientes.parquet')
#         groups_expedientes[procedure].to_parquet(expedientes_path, engine='pyarrow', index=False)
    
#     if procedure in groups_tramites:
#         tramites_path = os.path.join(procedure_folder, 'tramites.parquet')
#         groups_tramites[procedure].to_parquet(tramites_path, engine='pyarrow', index=False)
        
#         # Procesar trámites para generar el acumulado
#         df_tramites_proc = groups_tramites[procedure].copy()
        
#         # Ordenar por id_exp y fecha_tramite para calcular end_date
#         df_tramites_sorted = df_tramites_proc.sort_values(['id_exp', 'fecha_tramite'])
        
#         # Calcular end_date como la siguiente fecha_tramite del mismo id_exp
#         df_tramites_sorted['end_date'] = df_tramites_sorted.groupby('id_exp')['fecha_tramite'].shift(-1)
        
#         # Renombrar fecha_tramite a fecha
#         df_tramites_sorted.rename(columns={'fecha_tramite': 'fecha'}, inplace=True)
        
#         max_date = df_tramites_sorted['fecha'].max()
        
#         expanded_rows = []
#         for _, row in df_tramites_sorted.iterrows():
#             start = row['fecha']
#             end = row['end_date']
            
#             if pd.notnull(end):
#                 if end < start:
#                     end = start
#                 date_range = pd.date_range(start=start, end=end, freq='D')
#             else:
#                 date_range = pd.date_range(start=start, end=max_date, freq='D')
            
#             for date in date_range:
#                 expanded_rows.append({
#                     'fecha': date,
#                     'desc_tramite': row['desc_tramite']
#                 })
        
#         expanded_df = pd.DataFrame(expanded_rows)
        
#         if not expanded_df.empty:
#             aggregated_df = expanded_df.groupby(['fecha', 'desc_tramite']).size().reset_index(name='count')
#             pivot_df = aggregated_df.pivot(index='fecha', columns='desc_tramite', values='count').fillna(0).astype(int)
#             pivot_df.reset_index(inplace=True)
#         else:
#             pivot_df = pd.DataFrame(columns=['fecha'])
        
#         acumulado_path = os.path.join(procedure_folder, 'tramites_acumulado.parquet')
#         pivot_df.to_parquet(acumulado_path, engine='pyarrow', index=False)

# print("Procesamiento completado para todos los procedimientos.")        