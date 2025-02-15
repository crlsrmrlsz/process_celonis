# Procesamiento de datos de celonis  

Genera los siguientes ficheros:  

- expedientes_completo.parquet
- tramites_completo.parquet
- {codigo_procedmiento}/expedientes.parquet
- {codigo_procedmiento}/tramites.parquet
- {codigo_procedmiento}/tramites_acumulado.parquet
- {codigo_procedmiento}/estados_finales.csv
- data\apoyo\codigos_procedimientos.csv
- data\apoyo\expedientes_x_procedimiento.csv  
  
Ejecutar en orden:  
- **agrega_datos_celonis.py**, para generar expedientes y tramites completo. necesita los parquet de celonis en la carpeta /celonis  
- **genera_parquet_x_procedimiento.py**, para generar expedientes y tramites de cada procedimiento y guardarlo en sus directorios. utiliza los parquet del total de expedientes y trámites del punto anterior.  
- **genera_estadosfinales_x_procedimiento.py**, para generar el csv con los estados finales de cada procedimiento, en su carpeta, con nombres simplificados. utiliza los parquet por procedimiento del punto anterior.  
- **genera_csv_cantidad_exp.py**, para generar un csv en el que ver , ordenado por consejería, los procedimientos que tienen más trámites
- **genera_acumulado.py**, genera el tramites_acumulado.parquet para los procedimientos en data\apoyo\codigos_procedimientos.csv, que es el mismo fichero que se utiliza en streamlit-dashboard para definir los procedimientos que se muestran en el combo inicial. Solo de genera de estos porque es muy costoso.
- 
  
