# Imports
from os import walk
import pymysql

########################################################################
# Utility
######################################################################## 

# A helper function that read the config file and load it to memory
def getConfig():
    
    # defaults
    dbConfig = {}
    dbConfig["host"] = "localhost"
    dbConfig["user"] = "root"
    dbConfig["password"] = ""
    dbConfig["port"] = 3306
    BATCH_SIZE=100
    
    try:
        with open("config.txt", "r", encoding="utf-8") as configFile:
            config = {}
            for line in configFile:
                line = line.strip().split("=")
                if line[0] != "":
                    config[line[0]] = line[1]
                    
        dbConfig["host"] = config["DB_HOST"]
        dbConfig["port"] = int(config["DB_PORT"])
        
        dbConfig["user"] = config["DB_USERNAME"]
        dbConfig["password"] = config["DB_PASSWORD"]
        
        dbConfig["dbName"] = config["DB_DBNAME"]
        BATCH_SIZE = int(config["DB_INSERT_BATCH_SIZE"])
        
    except Exception as e:
        print("Exception occured:{}".format(e))
        print("Config file not exist! Using default parameters" )
        
    return dbConfig, BATCH_SIZE


# a function that try to connec to the database
def connectToDatabaseAndGetCursor(dbConfig, dbName):
    db = None
    cursor = None
    fail = False
    
    try: 
        # try to conect to db
        db = pymysql.connect(host = dbConfig["host"], user = dbConfig["user"], password = dbConfig["password"], port = dbConfig["port"])
    except Exception as e:
        print("Exception occured:{}".format(e))
        fail = True
    else:
        cursor = db.cursor()
        # try to create a DB
        try:
            # use test database for testing
            cursor.execute("create database IF NOT EXISTS " + dbName)
            cursor.execute("use " + dbName)
                
        except Exception as e:
            print("Exception occured:{}".format(e))
            fail = True
    
    return db, cursor, fail