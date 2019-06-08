# Imports
from os import walk
import pymysql

########################################################################
# Function for importing data
########################################################################

# I put this as function to make testing easier
# rootDir is the directory where data and spec folder is located
def dataImports(db, cursor, BATCH_SIZE, rootDir):
    
    specDir = rootDir + "specs/"
    dataDir = rootDir + "data/"
    
    # List of Problematic file, for debugging
    problematicFile = []

    print("Selecting data to load...")
    dataToLoad, problematicFile = getDataToLoad(rootDir)

    print()
    print("Loading data...")
    
    # Prepare create table SQL statement for spec files
    for tableName in dataToLoad:
        sqlStatement = ""
        spec = []
        try:
            with open( specDir + tableName + ".csv", "r", encoding="utf-8") as specFile:
                spec, sqlStatement = prepareCreateTableStatement(tableName, specFile)

            # execute the create table statement
            cursor.execute(sqlStatement)
        except Exception as e:
            print("Exception occured:{}".format(e))
            print("Problem with creating table for definition", tableName, ", Skipping this specs" )
            problematicFile.append(specDir + tableName + ".csv")

            # since the data file is not processed as well
            for data in dataToLoad[tableName]:
                problematicFile.append(dataDir + data)
        else:
            # commit the create table statement
            db.commit()
            print("Table ready for", tableName)

            # Prepare insert data SQL statement for data files
            for data in dataToLoad[tableName]:
                with open(dataDir + data, "r", encoding="utf-8") as dataFile:
                    insertions = prepareInsertStatement(tableName, dataFile, BATCH_SIZE, spec)

                # Execute the data insertion
                totalCount = 0
                for batch in insertions:
                    try:
                        # execute the insert data statement
                        cursor.execute(batch["statement"])
                    except Exception as e:
                        print()
                        print("Exception occured:{}".format(e))
                        print("Problem with creating table for data file", data, ", Skipping this file and rollbacking this file" )
                        problematicFile.append(dataDir + data)

                        # if some data input fail, rollback changes in this file
                        db.rollback()
                        totalCount = 0
                        break
                    else:
                        totalCount += batch["count"]
                        
                # commit the insertion
                db.commit()
                print(totalCount, "rows from", data, "are inserted")
                
            print()

    return problematicFile

# Check the specs and data dir to pre choose which files to use in the importing process
def getDataToLoad(rootDir):
    
    specDir = rootDir + "specs/"
    dataDir = rootDir + "data/"
    
    dataToLoad = dict()
    problematicFile = []
    
    # Get list of definition files
    for (dirpath, dirnames, filenames) in walk(specDir):
        for file in filenames:
            dataToLoad[file[:-4]] = []

    # Get list of data  fiels and assign to its correspnding spec if any
    for (dirpath, dirnames, filenames) in walk(dataDir):
        for file in filenames:
            tableName = file[:-15]

            # Ignore file without corresponding spec file and mark it as problematic
            if tableName in dataToLoad.keys():
                dataToLoad[tableName].append(file)
            else:
                print("Ignoring data file", file, "as no corresponding spec file exist")
                problematicFile.append(dataDir + file)

    # Preventing dictionary changed size during iteration error
    ignoredSpecList = []
    for dataFile in dataToLoad:
        if len(dataToLoad[dataFile]) == 0 :
            ignoredSpecList.append(dataFile)
            print("Ignoring spec file", dataFile + ".csv" , "as no corresponding data file exist")
            problematicFile.append(specDir + dataFile + ".csv")

    # Ignore spec file with no data file
    for dataFile in ignoredSpecList:
        del dataToLoad[dataFile]

    return dataToLoad, problematicFile

# Process spec file, returning the specification elements and create table statement
def prepareCreateTableStatement(tableName, specFile):
    spec = []
    
    # Skip the header
    next(specFile)

    # Prepare statement with id as primary key
    sqlStatement = "CREATE TABLE IF NOT EXISTS `" + tableName + "` ("
    
    # Naming primary key to tablename_id format to make working with it easier            
    sqlStatement += "`" + tableName + "_id` INT(11) NOT NULL auto_increment, "

    for line in specFile:
        specAttr = {}
        line = line.strip().split(',')

        # Formatting, personally I prefer all DB field name to be standardized
        # to lowercase and snake case
        colName = line[0].strip().lower().replace(" ", "_")
        length = line[1].strip()
        dataType = line[2].strip()

        specAttr["colName"] = colName
        specAttr["length"] = int(length)
        specAttr["type"] = dataType

        sqlStatement += "`" + colName + "` "

        if dataType == "TEXT":
            sqlStatement += "VARCHAR(" + length + ")"
        elif dataType == "FLOAT":
            sqlStatement += "DOUBLE"
        elif dataType == "DATETIME":
            sqlStatement += "VARCHAR(20)"
        elif dataType == "BOOLEAN":
            sqlStatement += "tinyint(1)"
        elif dataType == "INTEGER":
            # Might need to handle different number of digits in the future here                
            sqlStatement += "INT(" + length + ")"
        else:
            # Unknown Integer!
            raise Exception('unknown format in spec file ' + tableName)

        # Might need to add default value for table here  
        sqlStatement += ", "
        spec.append(specAttr)

    sqlStatement += "PRIMARY KEY (`" + tableName + "_id`) );"
    
    return spec, sqlStatement

# Process Data file input and prepare sql insert statements
# it will break down large file input into batch to prevent mysql error
def prepareInsertStatement(tableName, dataFile, batchSize, spec):
    
    # Prepare the header
    columnList = [] 
    for specAttr in spec:
        columnList.append(specAttr["colName"])
    insertColumnStatement = "INSERT INTO " + tableName + " (" + ", ".join(columnList) + ") VALUES"
    
    insertion = []
    
    count = 0
    rows = []

    for line in dataFile:  
        position = 0
        
        fieldList = []
        for field in spec:
            value = line[position : (position + field["length"])] 
            
            if field["type"] == "TEXT" or field["type"] == "DATETIME" :
                fieldList.append("'" + value.strip() + "'")
            else :
                fieldList.append(value.strip())
                
            position += field["length"] 

        rows.append("(" + ", ".join(fieldList) + ")")
        count += 1
        
        if count >= batchSize:
            # when the data get too big, separate it to different statements
            batch = {}
            batch["count"] = count
            batch["statement"] = insertColumnStatement + ", ".join(rows)
            insertion.append(batch)
            
            count = 0
            rows = []
    
    if count > 0 : 
        batch = {}
        batch["count"] = count
        batch["statement"] = insertColumnStatement + ", ".join(rows)
        insertion.append(batch)

    return insertion