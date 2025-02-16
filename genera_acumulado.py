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
folder_path_tratados_acc = 'data/tratadosacumulado'
file_tramites = 'tramites.parquet'
df_procedimientos = pd.read_csv(os.path.join(folder_path_apoyo, 'codigos_procedimientos.csv'),
                                encoding='utf-8', sep=';')

codigos_procedimientos = df_procedimientos['codigo_procedimiento'].unique()

for codigo_procedimiento in codigos_procedimientos:
    print(f"Procesando procedimiento {codigo_procedimiento}")
    
    folder_procedimiento = os.path.join(folder_path_tratados, str(codigo_procedimiento))
    df_tramites = pd.read_parquet(os.path.join(folder_procedimiento, file_tramites), engine='pyarrow')
    
    df_tramites = df_tramites[['id_exp', 'fecha_tramite', 'num_tramite', 'unidad_tramitadora']]
    # Step 1: Identify the ids with conversion failures
    failed_mask = pd.to_datetime(df_tramites["fecha_tramite"], errors="coerce").isna()
    failed_ids = df_tramites.loc[failed_mask, "id_exp"].unique()
    
    # Step 2: Filter out rows with these ids
    df_tramites = df_tramites[~df_tramites["id_exp"].isin(failed_ids)]
    
    # Optional: Convert fecha_tramite to datetime.date for the remaining rows
    df_tramites["fecha_tramite"] = pd.to_datetime(df_tramites["fecha_tramite"]).dt.date
    
    print(f"expedientes inicial: {len(df_tramites['id_exp'].unique())}")
    # Eliminar datos anteriores a 2010
    threshold_date = pd.to_datetime("2015-01-01").date()
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
    
    
    # Seleccionar columnas y ordenar
    df_tramites = df_tramites.sort_values(['id_exp', 'unidad_tramitadora', 'fecha_tramite' ]).reset_index(drop=True)
    
    
    
    # Calcular 'end_date' usando shift
    df_tramites['end_date'] = df_tramites.groupby(['id_exp', 'unidad_tramitadora'])['fecha_tramite'].shift(-1)
    # para cuando el cambio de estado es en el mismo día, la fecha fin del estado es el mismo día
    df_tramites['end_date'] = np.where(
        df_tramites['end_date'] < df_tramites['fecha_tramite'],
        df_tramites['fecha_tramite'],
        df_tramites['end_date']
    )
    
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
    #expanded_df = df_tramites.explode('date_range')[['id_exp', 'unidad_tramitadora',  'num_tramite', 'fecha_tramite', 'end_date', 'date_range']].reset_index(drop=True)
    expanded_df = df_tramites.explode('date_range')[['date_range', 'unidad_tramitadora', 'num_tramite']].reset_index(drop=True)
    
    expanded_df.rename(columns={'date_range': 'fecha_tramite'}, inplace=True)
    print("expanded_df")
    # pd.set_option('display.max_columns', None)  # Show all columns
    # print(expanded_df.head())
    # df_test_exp = expanded_df.head(20)
    if not expanded_df.empty:
        expanded_df = expanded_df.sort_values(['fecha_tramite', 'unidad_tramitadora', 'num_tramite']).reset_index(drop=True)
        aggregated_df = expanded_df.groupby(['fecha_tramite', 'unidad_tramitadora', 'num_tramite']).size().reset_index(name='count')
        pivot_df = aggregated_df.pivot(index=['fecha_tramite', 'unidad_tramitadora'], columns='num_tramite', values='count').fillna(0).astype(int)
        pivot_df.reset_index(inplace=True)
        # df_test_pivot = pivot_df.head(20)
        print("pivot_df")
        # print(pivot_df.head())
        
        folder_procedimiento_acc = os.path.join(folder_path_tratados_acc, str(codigo_procedimiento))
        
        acumulado_path = os.path.join(folder_procedimiento_acc, 'tramites_acumulado.parquet')
        pivot_df.to_parquet(acumulado_path, engine='pyarrow', index=False)
        print("generado parquet diario")
        # ---- New Code: Aggregación semanal ----
        # Creamos una columna 'week' que representa el inicio de la semana para cada fecha.
        # Aquí usamos el inicio de la semana (lunes) como referencia.
        pivot_df['semana'] = pivot_df['fecha_tramite'].dt.to_period('W').apply(lambda r: r.start_time)
        
        # Agrupamos por la semana y la unidad tramitadora, y seleccionamos la última observación (último día de la semana)
        weekly_df = pivot_df.sort_values('fecha_tramite').groupby(['semana', 'unidad_tramitadora']).last().reset_index()
        
        # Opcional: si prefieres que la columna de fecha se llame 'fecha_tramite' en lugar de 'week', puedes renombrarla:
        weekly_df.drop(columns=['fecha_tramite'], inplace=True)
        
        # Guardamos el DataFrame semanal en un archivo parquet
        acumulado_semanal_path = os.path.join(folder_procedimiento_acc, 'tramites_acumulado_semanal.parquet')
        weekly_df.to_parquet(acumulado_semanal_path, engine='pyarrow', index=False)
        print("generado parquet semanal")