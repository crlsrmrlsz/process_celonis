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