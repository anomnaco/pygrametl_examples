import datetime
import sys
import time
import psycopg2
import pygrametl
from pygrametl.datasources import TypedCSVSource, HashJoiningSource, FilteringSource, TransformingSource, RoundRobinSource
from pygrametl.tables import CachedDimension, SnowflakedDimension,\
    SlowlyChangingDimension, BulkFactTable

# Connection to target DW:
pgconn = psycopg2.connect()
connection = pygrametl.ConnectionWrapper(pgconn)
connection.setasdefault()
connection.execute('set search_path to wines')

# Methods
def pgcopybulkloader(name, atts, fieldsep, rowsep, nullval, filehandle):
    global connection
    curs = connection.cursor()
    curs.copy_from(file=filehandle, table=name, sep=fieldsep,
                   null=str(nullval), columns=atts)

global id_counter
id_counter = 0

def calculatemaxacidity(row):
    row['MaxAcidity'] = row['FixedAcidity']+row['VolatileAcidity']
    return row

def calculatelockedso2(row):
    row['LockedSO2'] = row['TotalSO2']-row['FreeSO2']
    return row

def redwinefilter(row):
    return row['Quality'] > 3

def whitewinefilter(row):
    return row['Quality'] > 4

def addIdRed(row):
    #print(row)
    global id_counter
    row['id'] = id_counter
    row['type'] = "Red"
    id_counter += 1
    return row

def addIdWhite(row):
    global id_counter
    row['id'] = id_counter
    row['type'] = "White"
    id_counter += 1
    return row
# Dimension and fact table objects





# Data sources - change the path if you have your files somewhere else
redwines = TypedCSVSource(f=open('/workspace/pygrametl_examples/data/winequality-red.csv', 'r', 16384),
                        casts={'FixedAcidity': float,'VolatileAcidity': float,'CitricAcid': float,'Sugar': float,'Chlorides': float,'FreeSO2': float,'TotalSO2': float,'Density':float,'pH': float,'Sulfates': float,'Alcohol': float,'Quality': int},
                        delimiter=',')

redwinestransformed = TransformingSource(redwines, addIdRed)

redwinesfiltered = FilteringSource(source=redwinestransformed, filter=redwinefilter)

whitewines = TypedCSVSource(f=open('/workspace/pygrametl_examples/data/winequality-white.csv', 'r', 16384),
                        casts={'FixedAcidity': float,'VolatileAcidity': float,'CitricAcid': float,'Sugar': float,'Chlorides': float,'FreeSO2': float,'TotalSO2': float,'Density':float,'pH': float,'Sulfates': float,'Alcohol': float,'Quality': int},
                        delimiter=',')

whitewinestransformed = TransformingSource(whitewines, addIdWhite)

whitewinesfiltered = FilteringSource(source=whitewinestransformed, filter=whitewinefilter)

#inputdata = HashJoiningSource(redwinesfiltered, 'id', whitewinesfiltered, 'id')
inputdata = RoundRobinSource((redwinesfiltered, whitewinesfiltered), batchsize=20)


for row in inputdata:
    calculatemaxacidity(row)
    calculatelockedso2(row)
    #print(row)
    with open('winesoutput.txt','a') as f:
        f.write(str(row))
        f.write("\n")