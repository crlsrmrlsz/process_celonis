# Procesamiento de datos de celonis  

Genera los siguientes ficheros:  

- expedientes_completo.parquet
- tramites_completo.parquet
- {codigo_procedmiento}/expedientes.parquet
- {codigo_procedmiento}/tramites.parquet
- {codigo_procedmiento}/estados_finales.csv
- data\apoyo\codigos_procedimientos.csv
- data\apoyo\expedientes_x_procedimiento.csv  
  
Ejecutar en ordeN:  
- **agrega_datos_celonis.py**, para generar expedientes y tramites completo
- **genera_parquet_x_procedimiento.py**, para generar expedientes y tramites de cada procedimiento y guarfarlo en sus directorios
- **genera_estadosfinales_x_procedimiento.py**, para generar el csv con los estados finales de cada procedimiento, en su carpeta, con nombres simplificados
- **genera_csv_cantidad_exp.py**, para generar un csv en el que ver , ordenado por consejería, los procedimientos que tienen más trámites
- 
