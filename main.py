# Imports
from os import walk
import pymysql
import dataLoader
import utils

########################################################################
# Entry point
# Run by using `python3 dataLoader.py` in bash
########################################################################
def main():
    
    # CONFIG 
    dbConfig, BATCH_SIZE = utils.getConfig()
    
    # Connect to Database
    db, cursor, fail = utils.connectToDatabaseAndGetCursor(dbConfig, dbConfig["dbName"])
    
    # End program run when it cannot connect to DB
    if not fail:
        problematicFile = dataLoader.dataImports(db, cursor, BATCH_SIZE, "")
        db.close()
        
        # Show all ignored files
        if len(problematicFile)  == 0:
            print("All file loaded successfully")
        else:
            print("Some file/specs are not loaded, here is the list of unprocessed data")
            for file in problematicFile:
                print(file)
                
main()