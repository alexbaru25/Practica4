# Practica4
README - Análisis de datos de bicimad
Trabajo realizado por Irma Alonso Sánchez, Alejandro Barragán Ruiz y Germán Sánchez Cuesta.
Dentro del archivo practica4_final.py se describe lo siguiete:

Importación de bibliotecas y creación del contexto Spark:
Se importa la biblioteca pyspark.
Se crea el contexto Spark utilizando SparkContext().

Carga de datos:
     Se carga el archivo JSON llamado 'sample_10e4.json' utilizando sc.textFile() y se almacena en el RDD rdd_base.

Búsqueda de la estación más deficitaria y menos deficitaria:
     Se define la función mapper() para extraer los campos relevantes de cada línea del archivo JSON.
     Se aplica la función mapper() al RDD rdd_base utilizando map(), creando un nuevo RDD rdd.
     Se definen las funciones contar(), mimin(), y mimax() para realizar cálculos posteriores.
     Se utiliza flatMap(), reduceByKey(), y reduce() para encontrar la estación más deficitaria y la estación más eficiente.

Análisis de transferencia de bicicletas entre estaciones:
     Se realiza un filtrado y conteo de bicicletas que salen de la estación 108 y llegan a la estación 135, creando los diccionarios d1 y d2.
     Se define la función traspaso_bicis() para obtener una lista de tuplas que representan las estaciones de transferencia y el número de bicicletas que las utilizan.

Análisis de tiempos de viaje de bicicletas:
     Se define la función mapper1() para extraer los campos relevantes de cada línea del archivo JSON.
     Se utiliza map(), reduceByKey(), y collect() para obtener la suma del tiempo de viaje de las bicicletas que salen de cada estación.
     Se filtra y muestra el tiempo de viaje de las bicicletas que salen de las estaciones 108 y 135.
     Se utiliza reduce() para encontrar la estación desde la cual salen las bicicletas con el tiempo de viaje máximo y mínimo.
     Se realiza un filtrado por tiempo de viaje mayor a 100,000 y se muestra el resultado.

Análisis del tiempo medio de viaje de bicicletas por estación:
     Se redefine la función mapper1() para extraer los campos relevantes de cada línea del archivo JSON.
     Se utiliza map(), groupByKey(), y reduce() para calcular el tiempo medio de viaje de las bicicletas que salen de cada estación.
     Se utiliza reduce() para encontrar la estación con el mayor y menor tiempo medio de viaje.

Análisis de usuarios basado en el código postal:
     Se redefine la función mapper1() para extraer los campos relevantes de cada línea del archivo JSON.
     Se utiliza map(), countByKey(), y funciones auxiliares para contar el número de usuarios que pertenecen a Madrid capital y los que no pertenecen.

El archivo practica4_final.ipynb contiene el mismo código que practica4_final.py pero para poderse visualizar en jupyter notebook.
La imagen Ejecucion_cluster.png muestra la ejecución del archivo practica4_final.py en el cluster proporcionado por el profesor.
El archivo practica4_final.txt muestra el resultado de ejecutar practica4_final.py.
