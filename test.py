# Imports
from os import walk
import pymysql
import dataLoader
import utils

########################################################################
# Test Files
########################################################################
def integrationTest():
    
    # TEST RESULT
    success = True
    
    # CONFIG 
    dbConfig, BATCH_SIZE = utils.getConfig()
    
    # Run the test cases
    success = runTestCase(happyFlow, dbConfig, 100) and success
    success = runTestCase(catchNonMatchingSpecAndDataFile, dbConfig, 100) and success
    success = runTestCase(rollbackWhenDataIsCorrupted, dbConfig, 100) and success
    
    if success :
        print("TEST CASE PASS")
        print("Yay! all test case passed")
    else:
        print("TEST CASE FAIL")
        print("Noooo! you broke something")

# Happy Flow test case
def happyFlow(testName, db, cursor, BATCH_SIZE):
    problematicFile = dataLoader.dataImports(db, cursor, 100, "testData/" + testName + "/")
    assert len(problematicFile) == 0
    
    # Baseline test
    cursor.execute("SELECT * from testformat1")
    rows = cursor.fetchall()
    assert str(rows) == "((1, 'Yooofrn', 1, 1), (2, 'Zneabar', 0, -12), (3, 'Uuietdxuq', 1, 103))", "testformat1"
    
    # test if it can success fully insert large amount of data (batch size is 100)
    cursor.execute("SELECT * from testformat2")
    rows = cursor.fetchall()
    assert len(rows) == 1152, "testformat2"
    
    # test if it can handle floating point and other data types
    cursor.execute("SELECT * from claims")
    rows = cursor.fetchall()
    assert str(rows) == "((1, '1234567890', 15.5, '2015-06-08T10:08:03Z', 0, 'Stephen'), (2, '1234567891', -15.6, '2015-06-18T10:08:03Z', 1, 'Curry'))", "claims"

# Happy Flow test case
def catchNonMatchingSpecAndDataFile(testName, db, cursor, BATCH_SIZE):
    problematicFile = dataLoader.dataImports(db, cursor, BATCH_SIZE, "testData/" + testName + "/")
    assert len(problematicFile) == 6, "there should be 6 ignored files"
    
    # No spec file
    assert problematicFile[0] == 'testData/catchNonMatchingSpecAndDataFile/data/testformat2_2015-06-28.txt',"problematicFile output"
    # No data file
    assert problematicFile[1] == 'testData/catchNonMatchingSpecAndDataFile/specs/claims.csv',"problematicFile output"
    # Spec file corrupt
    assert problematicFile[2] == 'testData/catchNonMatchingSpecAndDataFile/specs/testformat3.csv', "problematicFile output"
    assert problematicFile[3] == 'testData/catchNonMatchingSpecAndDataFile/data/testformat3_2015-06-29.txt', "problematicFile output"
    assert problematicFile[4] == 'testData/catchNonMatchingSpecAndDataFile/data/testformat3_2015-06-28.txt', "problematicFile output"
    assert problematicFile[5] == 'testData/catchNonMatchingSpecAndDataFile/data/testformat3_2015-06-30.txt', "problematicFile output"
        
    # Spec file corrupted
    cursor.execute("SELECT * FROM information_schema.tables " + 
                   "WHERE table_schema = 'testDB' AND table_name = 'testformat3' LIMIT 1;")
    
    rows = cursor.fetchall()
    print(rows)
    assert len(rows) == 0, "no table should exist as spec file is corrupted"
    
# Happy Flow test case
def rollbackWhenDataIsCorrupted(testName, db, cursor, BATCH_SIZE):
    problematicFile = dataLoader.dataImports(db, cursor, BATCH_SIZE, "testData/" + testName + "/")
    assert len(problematicFile) == 2, "problematicFile"
    assert problematicFile[0] == 'testData/rollbackWhenDataIsCorrupted/data/testformat1_2015-06-28.txt', "problematicFile"
    assert problematicFile[1] == 'testData/rollbackWhenDataIsCorrupted/data/testformat2_2015-06-28.txt', "problematicFile"
       
    # Line 177 is corrupted
    cursor.execute("SELECT * from testformat1")
    rows = cursor.fetchall()
    assert len(rows) == 0, "testformat1"
    
    # first line is corrupted
    cursor.execute("SELECT * from testformat2")
    rows = cursor.fetchall()
    assert len(rows) == 0, "testformat2"
    
# Helper function to run the test cases
def runTestCase(f, dbConfig, BATCH_SIZE) : 
    testName =  f.__name__
    # Connect to Database and create a fresh database on start of each test
    db, cursor, fail = utils.connectToDatabaseAndGetCursor(dbConfig, "testDB")
    
    try:
        f(testName, db, cursor, BATCH_SIZE)
    except AssertionError as e:
        print("ASSERTION ERROR occured:{}".format(e))
        success = False
    except Exception as e:    
        print("Exception occured:{}".format(e))
        success = False
    else:
        success = True
    
    if success:
        print("PASS --- " + testName)
    else:
        print("FAIL --- " + testName)
        
    # delete the database after each test
    cursor.execute("DROP DATABASE `testDB`;")
    db.close()
    print()
    return success

integrationTest()