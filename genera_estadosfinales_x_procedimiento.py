# -*- coding: utf-8 -*-
"""
Created on Sat Feb  1 19:34:16 2025

@author: flipe
"""

import pandas as pd
import os
import re

folder_path_celonis = 'data/celonis'
folder_path_tratados = 'data/tratados'
filename_procedimientos_definidos = 'TramitaGlobal-GB_PROCEDIMIENTOS_DEFINIDOS.parquet'
#filename_procedimientos_definidos_grp = 'TramitaGlobal-GB_GRP_PROCEDIMIENTOS_DEFINIDOS.parquet'


# Ruta completa del archivo
file_path_proc_definidos = os.path.join(folder_path_celonis, filename_procedimientos_definidos)
# Cargar el archivo Parquet con las columnas seleccionadas
df = pd.read_parquet(file_path_proc_definidos)

df['NUMTRAM_ORI']=df['NUMTRAM_ORI'].astype(int)
df['NUMTRAM_DES']=df['NUMTRAM_DES'].astype(int)
df['NPROCADMI']=df['NPROCADMI'].astype(int)
# # Ruta completa del archivo
# file_path_proc_definidos_grp = os.path.join(folder_path_celonis, filename_procedimientos_definidos_grp)
# # Cargar el archivo Parquet con las columnas seleccionadas
# df_procedimientos_definidos_grp = pd.read_parquet(file_path_proc_definidos_grp)


# Define the mapping dictionary for simplifications
simplifications = {
    "Resolución": "Res.",
    "Resolucion": "Res.",
    "Propuesta": "Prop.",
    "Recurso": "Rec.",
    "Desistimiento": "Desist.",
    "Desestimatoria": "Desest.",
    "Evaluación": "Eval.",
    "Liquidacion": "Liq.",
    "Liquidación": "Liq.",
    "Justificación": "Just.",
    "Solicitud": "Sol.",
    "Requerimiento": "Req.",
    "Abono": "Ab.",
    "Pago": "Pg.",
    "Pérdida": "Pérd.",
    "Trámite": "Trám.",
    "Inicio": "Inic.",
    "Derecho": "Der.",
    "Cobro": "Cob.",
    "Prórroga": "Prór.",
    "Provisional": "Prov.",
    "Denegatoria": "Deneg.",
    "Concesión": "Conc.",
    "Falta": "Falt.",
    "Informe": "Inf.",
    "Audiencia": "Aud.",
    "Inadmisión": "Inadm.",
    "Estimatoria": "Est.",
    "Documentación": "Doc.",
    "Definitiva": "Def.",
    "Trimestre": "Trim.",
    "Emplazamiento": "Empl.",
    "Registro": "Reg.",
    "Censo": "Cens.",
    "Resoluciones": "Res.",
    "Propuestas": "Prop.",
    "Recursos": "Rec.",
    "Presentaciones": "Pre.",
    "Desistimientos": "Desist.",
    "Desestimatorias": "Desest.",
    "Evaluaciones": "Eval.",
    "Liquidaciones": "Liq.",
    "Justificaciones": "Just.",
    "Solicitudes": "Sol.",
    "Requerimientos": "Req.",
    "Abonos": "Ab.",
    "Pagos": "Pg.",
    "Pérdidas": "Pérd.",
    "Trámites": "Trám.",
    "Inicios": "Inic.",
    "Derechos": "Der.",
    "Cobros": "Cob.",
    "Prórrogas": "Prór.",
    "Provisionales": "Prov.",
    "Denegatorias": "Deneg.",
    "Concesiones": "Conc.",
    "Faltas": "Falt.",
    "Informes": "Inf.",
    "Audiencias": "Aud.",
    "Inadmisiones": "Inadm.",
    "Estimatorias": "Est.",
    "Documentaciones": "Doc.",
    "Definitivas": "Def.",
    "Trimestres": "Trim.",
    "Emplazamientos": "Empl.",
    "Registros": "Reg.",
    "Censos": "Cens.",
    "Notificación":"Notif.",
    "Notificacion":"Notif.",
    "Devolución":"Dev.",
    "justificación":"justif.",
    "Procedimiento":"proc.",
    "Pendiente":"Pend.",
    "Presentación": "Present.",
    "Presentacion": "Present.",
    "Requisitos":"Req.",
    "resolución":"res.",
    "resolucion":"res.",
    "EVALUACIÓN":"Ev.",
    # Remove common prepositions and articles
    " de ": " ",
    " la ": " ",
    " del ": " ",
    " al ": " ",
    " por ": " ",
    " a ": " ",
    " y ": " ",
    " el ": " ",
    " en ": " ",
    " con ": " ",
    " de la ": " ",

    # Normalize multiple spaces
    "  ": " "
}


# Compile a regex pattern that matches any key in the dictionary.
# re.escape ensures that any special characters in the keys are properly escaped.
pattern = re.compile("|".join(map(re.escape, simplifications.keys())))

def simplify_denom(denom):
    """
    Replace all occurrences of substrings defined in the simplifications dictionary
    with their corresponding simplified versions.
    """
    # Use the pattern's sub method with a lambda function for replacement.
    return pattern.sub(lambda m: simplifications[m.group(0)], denom)


process_ids = df['NPROCADMI'].unique()


# Process each process type separately
for proc_id in process_ids:
    # Filter the DataFrame to get only rows for this process type
    df_proc = df[df['NPROCADMI'] == proc_id].copy()
    
    # --- Step 1: Identify origin states ---
    # Select the columns representing the origin state and remove duplicates.
    origins = df_proc[['NUMTRAM_ORI', 'DENOMINACION_ORIGEN']].drop_duplicates()
    # Rename columns for consistency.
    origins = origins.rename(columns={
        'NUMTRAM_ORI': 'NUMTRAM', 
        'DENOMINACION_ORIGEN': 'DENOMINACION'
    })
    
    # --- Step 2: Identify destination states ---
    # Select the columns representing the destination state and remove duplicates.
    destinations = df_proc[['NUMTRAM_DES', 'DENOMINACION_DESTINO']].drop_duplicates()
    # Rename columns for consistency.
    destinations = destinations.rename(columns={
        'NUMTRAM_DES': 'NUMTRAM', 
        'DENOMINACION_DESTINO': 'DENOMINACION'
    })
    
    # --- Step 3: Determine final states ---
    # Build sets of states (as tuples of (NUMTRAM, DENOMINACION)) for origin and destination.
    origin_set = set(origins.apply(lambda row: (row['NUMTRAM'], row['DENOMINACION']), axis=1))
    destination_set = set(destinations.apply(lambda row: (row['NUMTRAM'], row['DENOMINACION']), axis=1))
    
    # A state is final if it is in the destination set but not in the origin set.
    final_set = destination_set - origin_set
    
    # --- Step 4: Build the complete list of states for this process ---
    # We take the union of origins and destinations to cover all states.
    all_states = pd.concat([origins, destinations]).drop_duplicates().reset_index(drop=True)
    
    # Mark each state as final (1) or not (0)
    all_states['FINAL'] = all_states.apply(
        lambda row: 1 if (row['NUMTRAM'], row['DENOMINACION']) in final_set else 0,
        axis=1
    )
    
    # (Optional) Sort the states by NUMTRAM for readability
    all_states = all_states.sort_values(by=['FINAL', 'NUMTRAM'], ascending=[False, False])
    
    # Apply the simplification function to create a new column.
    all_states['DENOMINACION_SIMPLE'] = all_states['DENOMINACION'].apply(simplify_denom)
    
    # --- Step 5: Export the CSV ---
    # Create a folder with the name of the process type (if it doesn't already exist)
    # Create a folder for the current value. Using os.path.join ensures correct path formatting.
    folder_name = os.path.join(folder_path_tratados, str(proc_id))

    os.makedirs(folder_name, exist_ok=True)
    
    # Define the output CSV path
    output_csv = os.path.join(folder_name, 'estados_finales.csv')
    
    # Export the DataFrame to CSV using utf-8-sig encoding so that applications like Excel can read it correctly.
    all_states.to_csv(output_csv, index=False, encoding='utf-8-sig', sep = ';')
    
    print(f'Process {proc_id}: CSV written to {output_csv}')




