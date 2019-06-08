##To start using:
1. You must have mysql running and put the connection details in the config.txt
2. Make sure you have pymysql library
3. Then, run the main file using below command
```
python3 main.py
```

##To run integration test:
```
python3 test.py
```


##Folder Structure:
- data - this is the folder housing the data files
- specs - this is the folder housing the spec files
- testdata - this is the folder for housing the test data

- dataLoader.ipynb -  this is the program in jupyter notebook format, which is easier to debug

- config.txt - config file housing your database connection and batch size configurations
- dataLoader.py - the main file, holds the logic to insert those in data/ and specs/ to database
- utils.py - utility function library
- main.py - entry point of the program
- test.py - integration test script

## Assumption:
- If there is no corresponding spec file, it will be ignored
- If a spec file have no data, the table will not be created
- Problematic files should not stop the program from running
- Since we are using MYSQL, boolean types are stored as tinyint(1)
- We split data sql insertion into batch to prevent mysql errors
- When any of them fail, the whole file will not be inputted and any inserted file should be rolled back

## Standards applied:
- ISO_8601 for datetime input
- database table field name is in lowercase and snake case
- database primary key (the id) is in tablename_id format

## Future improvement (might be overengineering):
- handling interger with large digits:
- using library (pandas?) to handle statement preparation
- default value for database table fields?
- Support to insert to another DB type such as MongoDB
- Disable console output when running test