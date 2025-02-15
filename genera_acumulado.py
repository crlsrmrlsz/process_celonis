# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 06:38:20 2025

@author: flipe
"""

import pandas as pd
import os
import numpy as np

# leer csv para ver que procedimientos hay que calcular
folder_path_apoyo = 'data/apoyo'
folder_path_tratados = 'data/tratados'
file_tramites = 'tramites.parquet'
df_procedimientos = pd.read_csv(os.path.join(folder_path_apoyo, 'codigos_procedimientos.csv'),
                                encoding='utf-8', sep=';')

# codigo_procedimiento = '216'
# print(f"Procesando procedimiento {codigo_procedimiento}")

# folder_procedimiento = os.path.join(folder_path_tratados, codigo_procedimiento)
# df_tramites = pd.read_parquet(os.path.join(folder_procedimiento, file_tramites), engine='pyarrow')

# # Convertir 'fecha_tramite' a datetime (convertirá errores a NaT)
# df_tramites["fecha_tramite"] = pd.to_datetime(df_tramites["fecha_tramite"], errors="coerce")

# print(f"expedientes inicial: {len(df_tramites['id_exp'].unique())}")
# # Eliminar datos anteriores a 2010
# threshold_date = pd.to_datetime("2015-01-01")
# df_tramites = df_tramites.groupby("id_exp").filter(
#     lambda group: group["fecha_tramite"].min() >= threshold_date
# )
# print(f"expedientes tras filtro 2015 : {len(df_tramites['id_exp'].unique())}")
# # --- Correction Step ---
# # Drop groups that have any NaT in 'fecha_tramite'
# df_tramites = df_tramites.groupby("id_exp").filter(
#     lambda group: group["fecha_tramite"].notna().all()
# )

# df_tramites['unidad_tramitadora'] = df_tramites['unidad_tramitadora'].fillna('No especificada')

# print(f"expedientes tras filtro fehca not null : {len(df_tramites['id_exp'].unique())}")

# df_tramites = df_tramites.sort_values(by=["id_exp", "fecha_tramite", "unidad_tramitadora"])

# # Seleccionar columnas y ordenar
# df_tramites = df_tramites[['id_exp', 'fecha_tramite', 'num_tramite', 'unidad_tramitadora']].sort_values(['id_exp', 'fecha_tramite'])

# # Calcular 'end_date' usando shift
# df_tramites['end_date'] = df_tramites.groupby('id_exp')['fecha_tramite'].shift(-1)
# max_date = df_tramites['fecha_tramite'].max()

# # Limpiar 'end_date': llenar NaNs con max_date y asegurar que end_date >= fecha_tramite
# df_tramites['end_date'] = df_tramites['end_date'].fillna(max_date)
# df_tramites['end_date'] = np.where(df_tramites['end_date'] < df_tramites['fecha_tramite'],
#                                   df_tramites['fecha_tramite'],
#                                   df_tramites['end_date'])

# # Generar rangos de fechas. Es recomendable crear una nueva columna para los rangos en lugar de sobrescribir 'fecha_tramite'
# df_tramites['date_range'] = df_tramites.apply(
#     lambda x: pd.date_range(start=x['fecha_tramite'], end=x['end_date'], freq='D'),
#     axis=1
# )

# # Explode los rangos de fecha y limpiar el DataFrame final
# expanded_df = df_tramites.explode('date_range')[['num_tramite', 'unidad_tramitadora', 'date_range']].reset_index(drop=True)
# expanded_df.rename(columns={'date_range': 'fecha_tramite'}, inplace=True)
# print("expanded_df")
# pd.set_option('display.max_columns', None)  # Show all columns
# print(expanded_df.head())
# df_test_exp = expanded_df.head(20)
# if not expanded_df.empty:
#     aggregated_df = expanded_df.groupby(['fecha_tramite', 'unidad_tramitadora', 'num_tramite']).size().reset_index(name='count')
#     pivot_df = aggregated_df.pivot(index=['fecha_tramite', 'unidad_tramitadora'], columns='num_tramite', values='count').fillna(0).astype(int)
#     pivot_df.reset_index(inplace=True)
#     df_test_pivot = pivot_df.head(20)
#     print("pivot_df")
#     print(pivot_df.head())
#     acumulado_path = os.path.join(folder_procedimiento, 'tramites_acumulado.parquet')
#     pivot_df.to_parquet(acumulado_path, engine='pyarrow', index=False)
# else:
#     print("No se generaron datos")

codigos_procedimientos = df_procedimientos['codigo_procedimiento'].unique()

for codigo_procedimiento in codigos_procedimientos:
    # codigo_procedimiento = '216'
    print(f"Procesando procedimiento {codigo_procedimiento}")
    
    folder_procedimiento = os.path.join(folder_path_tratados, str(codigo_procedimiento))
    df_tramites = pd.read_parquet(os.path.join(folder_procedimiento, file_tramites), engine='pyarrow')
    
    # Convertir 'fecha_tramite' a datetime (convertirá errores a NaT)
    df_tramites["fecha_tramite"] = pd.to_datetime(df_tramites["fecha_tramite"], errors="coerce")
    
    print(f"expedientes inicial: {len(df_tramites['id_exp'].unique())}")
    # Eliminar datos anteriores a 2010
    threshold_date = pd.to_datetime("2015-01-01")
    df_tramites = df_tramites.groupby("id_exp").filter(
        lambda group: group["fecha_tramite"].min() >= threshold_date
    )
    print(f"expedientes tras filtro 2015 : {len(df_tramites['id_exp'].unique())}")
    # --- Correction Step ---
    # Drop groups that have any NaT in 'fecha_tramite'
    df_tramites = df_tramites.groupby("id_exp").filter(
        lambda group: group["fecha_tramite"].notna().all()
    )
    
    df_tramites['unidad_tramitadora'] = df_tramites['unidad_tramitadora'].fillna('No especificada')
    
    print(f"expedientes tras filtro fehca not null : {len(df_tramites['id_exp'].unique())}")
    
    df_tramites = df_tramites.sort_values(by=["id_exp", "fecha_tramite", "unidad_tramitadora"])
    
    # Seleccionar columnas y ordenar
    df_tramites = df_tramites[['id_exp', 'fecha_tramite', 'num_tramite', 'unidad_tramitadora']].sort_values(['id_exp', 'fecha_tramite'])
    
    # Calcular 'end_date' usando shift
    df_tramites['end_date'] = df_tramites.groupby('id_exp')['fecha_tramite'].shift(-1)
    max_date = df_tramites['fecha_tramite'].max()
    
    # Limpiar 'end_date': llenar NaNs con max_date y asegurar que end_date >= fecha_tramite
    df_tramites['end_date'] = df_tramites['end_date'].fillna(max_date)
    df_tramites['end_date'] = np.where(df_tramites['end_date'] < df_tramites['fecha_tramite'],
                                      df_tramites['fecha_tramite'],
                                      df_tramites['end_date'])
    
    # Generar rangos de fechas. Es recomendable crear una nueva columna para los rangos en lugar de sobrescribir 'fecha_tramite'
    df_tramites['date_range'] = df_tramites.apply(
        lambda x: pd.date_range(start=x['fecha_tramite'], end=x['end_date'], freq='D'),
        axis=1
    )
    
    # Explode los rangos de fecha y limpiar el DataFrame final
    expanded_df = df_tramites.explode('date_range')[['num_tramite', 'unidad_tramitadora', 'date_range']].reset_index(drop=True)
    expanded_df.rename(columns={'date_range': 'fecha_tramite'}, inplace=True)
    print("expanded_df")
    # pd.set_option('display.max_columns', None)  # Show all columns
    # print(expanded_df.head())
    # df_test_exp = expanded_df.head(20)
    if not expanded_df.empty:
        aggregated_df = expanded_df.groupby(['fecha_tramite', 'unidad_tramitadora', 'num_tramite']).size().reset_index(name='count')
        pivot_df = aggregated_df.pivot(index=['fecha_tramite', 'unidad_tramitadora'], columns='num_tramite', values='count').fillna(0).astype(int)
        pivot_df.reset_index(inplace=True)
        # df_test_pivot = pivot_df.head(20)
        print("pivot_df")
        # print(pivot_df.head())
        acumulado_path = os.path.join(folder_procedimiento, 'tramites_acumulado.parquet')
        pivot_df.to_parquet(acumulado_path, engine='pyarrow', index=False)
    else:
        print("No se generaron datos")
















# para cada procedimiento
# codigos_procedimientos = df_procedimientos['codigo_procedimiento'].unique()


# codigo_procedimiento ='1033'
# print(f"Procesando procedimiento {str(codigo_procedimiento)}")
# folder_procedimiento = os.path.join(folder_path_tratados, str(codigo_procedimiento))
# df_tramites = pd.read_parquet(os.path.join(folder_procedimiento, file_tramites), engine='pyarrow')

# # seleccionar solo las columnas de interés
# df_tramites = df_tramites[['id_exp','fecha_tramite', 'num_tramite', 'unidad_tramitadora']]

# # Ordenar por id_exp y fecha_tramite para calcular end_date
# df_tramites_sorted = df_tramites.sort_values(['id_exp', 'fecha_tramite'])

# # Calcular end_date como la siguiente fecha_tramite del mismo id_exp
# df_tramites_sorted['end_date'] = df_tramites_sorted.groupby('id_exp')['fecha_tramite'].shift(-1)


# max_date = df_tramites_sorted['fecha_tramite'].max()

# expanded_rows = []
# for _, row in df_tramites_sorted.iterrows():
#     start = row['fecha_tramite']
#     end = row['end_date']

#     if pd.notnull(end):
#         if end < start:
#             end = start
#         date_range = pd.date_range(start=start, end=end, freq='D')
#     else:
#         date_range = pd.date_range(start=start, end=max_date, freq='D')

#     for date in date_range:
#         expanded_rows.append({
#             'fecha_tramite': date,
#             'num_tramite': row['num_tramite'],
#             'unidad_tramitadora': row['unidad_tramitadora']
#         })

# expanded_df = pd.DataFrame(expanded_rows)