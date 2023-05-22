from pyspark import SparkContext
sc = SparkContext()
import json

rdd_base = sc.textFile('sample_10e4.json')

#buscar la estación más deficitaria y menos deficitaria
def mapper(line):
    data = json.loads(line)
    start = data['idunplug_station']
    end = data['idplug_station']
    return start, end

rdd = rdd_base.map(mapper)

def contar(elem):
    return [(elem[0],-1),(elem[1],1)]

#funcón que calcula el minimo entre dos tuplas o listas, comparando el segundo elemento    
def mimin(x,y):
    if x[1]==min(x[1],y[1]):
        return x
    else:
        return y
#funcón que calcula el maximoentre dos tuplas o listas, comparando el segundo elemento
def mimax(x,y):
    if x[1]==min(x[1],y[1]):
        return y
    else:
        return x


print('Estacion mas deficitaria:',rdd.flatMap(lambda x: contar(x)).reduceByKey(lambda x,y:x+y).reduce(mimin))
print('Estacion mas eficiente:',rdd.flatMap(lambda x: contar(x)).reduceByKey(lambda x,y:x+y).reduce(mimax))

#obtenemos que la más deficitaria es la 108 y la menos la 135


d1=rdd.filter(lambda x: x[1]==135).countByKey() 
#diccionario en el que las llaves son las estaciones desde las que salen bicis hacia la 135 y 
#los valores el número de bicis que llegan desde esas estaciones


def flip(x):
    return (x[1],x[0])

d2=rdd.filter(lambda x: x[0]==108).map(flip).countByKey()
#diccionario en el que las llaves son las estaciones a las que van las bicis que salen desde la 108 y 
#los valores el número de bicis que llegan a esas estaciones

#dados los diccionarios anteriores, devuelve una lista de tuplas. El primer elemento de cada 
#son las estaciones por las que pasa una bici que sale de la estacion 108 y llega a la 135
# y el sgundo elemento es el número de bicis que hacen ese recorrido
def traspaso_bicis(d1,d2):
    
    lst=[]
    for i in d1.keys():
        if i in d2.keys():
            lst.append((i,min(d1[i],d2[i])))
    return lst

traspaso_bicis(d1,d2)


# nos quedamos con los datos de las estaciones de las que salen las bicis y el tiempo que han estado fuera.
def mapper1(line):
    data = json.loads(line)
    start = data['idunplug_station']
    time = data['travel_time']
    return start, time

#sumamos todos los tiempos de las bicis que salen de cada estacion
rdd1 = rdd_base.map(mapper1).reduceByKey(lambda x,y:x+y)
print("Estaciones y suma del tiempo de viaje de las bicis que salen de esas estaciones:")
print(rdd1.collect())

#vemos los tiempos de viaje de las bicis que salen de la estacion 108 y 135
rdd_108=rdd1.filter(lambda x: x[0]==108)
rdd_135=rdd1.filter(lambda x: x[0]==135)
print("Tiempo de viaje de las bicis que salen de la estacion 108:")
print(rdd_108.collect()[0][1])
print("Tiempo de viaje de las bicis que salen de la estacion 135:")
print(rdd_135.collect()[0][1])


rdd2=rdd1.reduce(mimax)
rdd3=rdd1.reduce(mimin)
print("Estacion desde la que salen las bicis con el maximo de tiempo de viaje:")
print(rdd2)
print("Estacion desde la que salen las bicis con el minimo de tiempo de viaje:")
print(rdd3)

print(rdd1.filter(lambda x: x[1]>100000).collect())


#Estudiamos el tiempo medio de viaje de las bicis que salen de cada estación.
def mapper1(line):
    data = json.loads(line)
    start = data['idunplug_station']
    time = data['travel_time']
    return start, time

#calculamos una lista con tuplas con la estación de la que salen las bicis y el tiempo medio de cada una.
bici=rdd_base.map(mapper1).groupByKey().map(lambda x: (x[0],sum(x[1])//len(x[1])))
print(bici.collect())

#En base a todos los datos, cual es la estacion con el mayor tiempo medio
mayor_uso=bici.reduce(mimax)[0]
print("Estacion con el mayor tiempo medio: ", mayor_uso)
#En base a todos los datos, cual es la estacion con el menor tiempo medio
menor_uso=bici.reduce(mimin)[0]
print("Estacion con el menor tiempo medio: ", menor_uso)

print("Basándonos en los resultados anteriores, vemos que las estaciones con mayor y menor tiempo medio no coinciden con las de mayor y menor tiempo total.")


#Vamos a obtener el número de gente que es de madrid y de fuera en base al zip_code (codigo postal)
#el cual nos da dicha informacion
def mapper1(line):
    data = json.loads(line)
    start = data['idunplug_station']
    codigo = data['zip_code']
    return codigo, start

#Primero agrupamos en base al codigo postal
rddd1 = rdd_base.map(mapper1).countByKey()#.filter(lambda x: x[0]=='28004')

#Calculamos los usuarios que pertenecen a Madrid capital
def de_Madrid():
    conteo=0
    for i in rddd1.keys():
        if i!='' and len(i)>4 and i[2]=='0':
            conteo+=rddd1[i]
    return conteo

#Calculamos los usuarios que pertenecen a fuera de Madrid capital
def fuera_de_Madrid():
    conteo=0
    for i in rddd1.keys():
        if i!='' and len(i)>4 and i[2]!='0':
            conteo+=rddd1[i]
    return conteo

Madrid=de_Madrid()
No_Madrid=fuera_de_Madrid()

print('Usuarios que pertenecen a Madrid capital:')
print(Madrid)
print('Usuarios que no pertenecen a Madrid capital:')
print(No_Madrid)
