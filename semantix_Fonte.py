# Databricks notebook source
#Marcelo Marquesini Reis Ribeiro

from pyspark import SparkConf, SparkContext
from operator import add
import pandas as pd

julho = sc.textFile('/FileStore/tables/access_log_Jul95')
julho = julho.cache()


agosto = sc.textFile('/FileStore/tables/access_log_Aug95')
agosto = agosto.cache()

julho_contador = julho.flatMap(lambda line: line.split(' ')[0]).distinct().count()
Agosto_contador = agosto.flatMap(lambda line: line.split(' ')[0]).distinct().count()

print('Hosts Julho: %s' % julho_contador)  #Resultado do valor 55
print('hosts agosto %s' % Agosto_contador) #Resultado do valor 53


def NUmeroErros(line):
    try:
        code = line.split(' ')[-2]
        if code == '404':
            return True
    except:
        pass
    return False
    
julho_404 = julho.filter(NUmeroErros).cache()
agosto_404 = agosto.filter(NUmeroErros).cache()

print('404 Julho: %s' % julho_404.count())
print('404 Agosto %s' % agosto_404.count())


def Top5_error(mes,rdd):
    pontosmapeados = rdd.map(lambda line: line.split('"')[1].split(' ')[1])
    Contador = pontosmapeados.map(lambda pontosmapeados: (pontosmapeados, 1)).reduceByKey(add)
    Topmax = Contador.sortBy(lambda pair: -pair[1]).take(5)
  
    for pontosmapeados, Contador in Topmax:
        print(mes,pontosmapeados, Contador) 
        
    return Topmax


Top5_error("Junho",julho_404)
Top5_error("Agosto",agosto_404)


def Contador_Pordia(rdd):
    Dias = rdd.map(lambda line: line.split('[')[1].split(':')[0])
    Contador = Dias.map(lambda Dias: (Dias, 1)).reduceByKey(add).collect()
    
    print('\\Erros por dia:')
    for Dias, count in Contador:
        print(Dias, count)
        
    return Contador

Contador_Pordia(julho_404)
Contador_Pordia(agosto_404)


def AcumuladodeBytes(rdd):
  def Contador_b(line):
      try:
          Contador = int(line.split(" ")[-1])
          if Contador < 0:
              raise ValueError()
          return Contador
      except:
          return 0

  Contador = rdd.map(Contador_b).reduce(add)
  return Contador


print('Total Bytes Junho: %s' % AcumuladodeBytes(julho))
print('Total Bytes Agosto: %s' % AcumuladodeBytes(agosto))

sc.stop()

