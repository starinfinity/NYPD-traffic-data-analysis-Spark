#@author:Rahul Gahlot <gahlotrl@mail.uc.edu>

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col
from multiprocessing import Pool

import datetime


#single Reducer function to add all values

def reduce_function(sub_mapping):
    result = {}
    for i in sub_mapping:
        for k, v in i.items():
            try:
                result[k] += v
            except KeyError:
                result[k] = v

    max_val = 0
    max_index = -1
    for i in result:
        if result[i] > max_val:
            max_val = result[i]
            max_index = i

    return max_index


#mapper for Date on which maximum number of accidents took place
def map_function1(words):
    result = {}
    try:
        result[words[0]] += words[3] + words[4]
    except KeyError:
        result[words[0]] = words[3] + words[4]

    return result

#Date on which maximum number of accidents took place.
def mapReduce1(allItems):
    pool = Pool(processes=3)

    sub_map_result = pool.map(map_function1, allItems)
    return reduce_function(sub_map_result)


#mapper for Borough with maximum count of accident fatality
def map_function2(words):
    result = {}
    try:
        result[words[1]] += words[4]
    except KeyError:
        result[words[1]] = words[4]

    return result

#Borough with maximum count of accident fatality
def mapReduce2(allItems):
    pool = Pool(processes=3)
    sub_map_result = pool.map(map_function2, allItems)
    return reduce_function(sub_map_result)


#mapper for Zip with maximum count of accident fatality
def map_function3(words):
    result = {}
    try:
        result[words[2]] += words[4]
    except KeyError:
        result[words[2]] = words[4]

    return result


#Zip with maximum count of accident fatality
def mapReduce3(allItems):
    pool = Pool(processes=3)
    sub_map_result = pool.map(map_function3, allItems)
    return reduce_function(sub_map_result)


#mapper for vehicle type is involved in maximum accidents
def map_function4(words):
    result = {}
    #print(words[0])
    try:
        result[words[11]] += words[3]+words[4]
    except KeyError:
        result[words[11]] = words[3]+words[4]

    return result

#vehicle type involved in maximum accidents
def mapReduce4(allItems):
    pool = Pool(processes=3)
    sub_map_result = pool.map(map_function4, allItems)
    return reduce_function(sub_map_result)

#mapper for maximum Number Of Persons and Pedestrians Injured
def map_function5(words):
    result = {}
    year = words[0].split('/')[2]
    try:
        result[year] += words[5]
    except KeyError:
        result[year] = words[5]

    return result

#Year in which maximum number of Pedestrians Injured
def mapReduce5(allItems):
    pool = Pool(processes=3)
    sub_map_result = pool.map(map_function5, allItems)
    return reduce_function(sub_map_result)

#mapper for maximum Number Of Persons and Pedestrians Killed	
def map_function6(words):
    result = {}
    year = words[0].split('/')[2]
    try:
        result[year] += words[6]
    except KeyError:
        result[year] =  words[6]

    return result

#Year in which maximum Number Of Persons and Pedestrians Killed
def mapReduce6(allItems):
    pool = Pool(processes=3)
    sub_map_result = pool.map(map_function6, allItems)
    return reduce_function(sub_map_result)

#mapper for maximum Number Of Cyclist Injured and Killed
def map_function7(words):
    result = {}
    year = words[0].split('/')[2]
    try:
        result[year] += words[7] + words[8]
    except KeyError:
        result[year] = words[7] + words[8]

    return result

#Year in which maximum Number Of Cyclist Injured and Killed
def mapReduce7(allItems):
    pool = Pool(processes=3)
    sub_map_result = pool.map(map_function7, allItems)
    return reduce_function(sub_map_result)

#mapper for maximum Number Of Motorist Injured and Killed 
def map_function8(words):
    result = {}
    year = words[0].split('/')[2]
    try:
        result[year] += words[9]+words[10]
    except KeyError:
        result[year] = words[9]+words[10]

    return result

#Year in which maximum Number Of Motorist Injured and Killed
def mapReduce8(allItems):
    pool = Pool(processes=3)
    sub_map_result = pool.map(map_function8, allItems)
    return reduce_function(sub_map_result)


if __name__ == '__main__':
    sqlContext = SQLContext(sc)
    #read the file from hadoop-gate
    df = sqlContext.read.load('hdfs:////user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt', format='com.databricks.spark.csv', header='true', inferSchema='true')
    df1 = df.filter(col('#DATE').isNotNull() & col('Borough').isNotNull() & col('Zip Code').isNotNull() & col(
        'Number Of Persons Injured').isNotNull() & col('Number Of Persons Killed').isNotNull() & col(
        'Number Of Pedestrians Injured').isNotNull() & col('Number Of Pedestrians Killed').isNotNull() & col(
        'Number Of Cyclist Injured').isNotNull() & col('Number Of Cyclist Killed').isNotNull() & col(
        'Number Of Motorist Injured').isNotNull() & col('Number Of Motorist Killed').isNotNull() & col(
        'Vehicle Type Code 1').isNotNull())

    

    #select the columns which we need
    df1.select('#Date', 'Borough', 'Zip Code', 'Number Of Persons Injured', 'Number Of Persons Killed',
              'Number Of Pedestrians Injured', 'Number Of Pedestrians Killed', 'Number Of Cyclist Injured',
              'Number Of Cyclist Killed', 'Number Of Motorist Injured', 'Number Of Motorist Killed',
             'Vehicle Type Code 1').write.format('csv').save('cleaninputfile.csv')
    df = sqlContext.read.load('cleaninputfile.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
    dataCollection = df.collect()
    maxIndex = mapReduce1(dataCollection)
	#save a result file to view the result
    f = open('resultoutput.txt', 'w')

#print the output on the console

    f.write('Date on which maximum number of accidents took place : ' + maxIndex+ '\n')
    print('Date on which maximum number of accidents took place : ' + maxIndex+ '\n')
    maxIndex = mapReduce2(dataCollection)

    f.write('Borough with maximum count of accident fatality : ' + maxIndex + '\n')
    print('Borough with maximum count of accident fatality : ' + maxIndex + '\n')
    maxIndex = mapReduce3(dataCollection)

    f.write('Zip with maximum count of accident fatality : ' + str(maxIndex) + '\n')
    print('Zip with maximum count of accident fatality : ' + str(maxIndex) + '\n')

    maxIndex = mapReduce4(dataCollection)
    f.write('Which vehicle type is involved in maximum accidents : ' + maxIndex + '\n')

    print('Which vehicle type is involved in maximum accidents : ' + maxIndex + '\n')

    maxIndex = mapReduce5(dataCollection)
    f.write('Year in which maximum Number Of Persons and Pedestrians Injured : ' + maxIndex + '\n')
    print('Year in which maximum Number Of Persons and Pedestrians Injured : ' + maxIndex + '\n')

    maxIndex = mapReduce6(dataCollection)
    f.write('Year in which maximum Number Of Persons and Pedestrians Killed : ' + maxIndex + '\n')
    print('Year in which maximum Number Of Persons and Pedestrians Killed : ' + maxIndex + '\n')

    maxIndex = mapReduce7(dataCollection)
    f.write('Year in which maximum Number Of Cyclist Injured and Killed (combined) : ' + maxIndex + '\n')
    print('Year in which maximum Number Of Cyclist Injured and Killed (combined) : ' + maxIndex + '\n')

    maxIndex = mapReduce8(dataCollection)
    f.write('Year in which maximum Number Of Motorist Injured and Killed (combined) : ' + maxIndex + '\n')
    print('Year in which maximum Number Of Motorist Injured and Killed (combined) : ' + maxIndex + '\n')