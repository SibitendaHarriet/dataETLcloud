from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode

dbName = 'sensordata'

TABLES = {}

TABLES['180stations'] = (
    "CREATE TABLE IF NOT EXISTS `sensordata`.`180stations` ("
    "  `ID` INT NOT NULL AUTO_INCREMENT,"
    "  `Fwy` INT NOT NULL,"
    "  `District` INT NOT NULL,"
    "  `City` INT DEFAULT NULL,"
    "  `State_PM` DOUBLE NOT NULL,"
    "  `Abs_PM` DOUBLE NOT NULL,"
    "  `Latitude` DOUBLE NOT NULL,"
    "  `Longitude` DOUBLE NOT NULL,"
    "  `Length` DOUBLE DEFAULT NULL,"
    "  `Type` TEXT NOT NULL,"
    "  `Lanes` INT NOT NULL,"
    "  `Name` TEXT NOT NULL,"
    "  `User_ID_1` TEXT DEFAULT NULL,"
    "  `User_ID_2` TEXT DEFAULT NULL,"
    "  `User_ID_3` TEXT DEFAULT NULL,"
    "  `User_ID_4` TEXT DEFAULT NULL,"
    "   PRIMARY KEY (`ID`)"
") ENGINE=InnoDB;")
    

TABLES['time'] = (
    "CREATE TABLE IF NOT EXISTS `sensordata`.`time` ("
    "  `idtime` INT NOT NULL AUTO_INCREMENT,"
    "  `date` DATE NOT NULL,"
    "  `dayofweek` INT NOT NULL,"
    "   `hour` INT NOT NULL,"
    "   `minute` INT NOT NULL,"
    "   `seconds` INT NOT NULL,"
    "   PRIMARY KEY (`idtime`)"
") ENGINE=InnoDB;")
    

TABLES['flow'] = (
    "CREATE TABLE IF NOT EXISTS `sensordata`.`flow` ("
    "   `flowid` INT NOT NULL AUTO_INCREMENT,"
    "   `idtime` INT NOT NULL,"
    "   `flow1` INT NOT NULL,"
    "   `flow2` INT NOT NULL,"
    "   `flow3` INT NOT NULL,"
    "   `flowtotal` INT NOT NULL,"
    "   PRIMARY KEY (`flowid`),"
    "   INDEX `timeflow_idx` (`idtime` ASC) VISIBLE,"
    "   CONSTRAINT `timeflow`"
            "FOREIGN KEY (`idtime`)"
            "REFERENCES `sensordata`.`time` (`idtime`)"    
") ENGINE=InnoDB;")



TABLES['occupancy'] = (
    "CREATE TABLE IF NOT EXISTS `sensordata`.`ocuppancy` ("
        "`idocuppancy` INT NOT NULL AUTO_INCREMENT,"
        "`idtime` INT NOT NULL,"
        "`ocuppancy1` INT NOT NULL,"
        "`ocuppancy2` INT NOT NULL,"
    "PRIMARY KEY (`idocuppancy`),"
    "INDEX `ocuptime_idx` (`idtime` ASC) VISIBLE,"
    "CONSTRAINT `ocuptime`"
        "FOREIGN KEY (`idtime`)"
        "REFERENCES `sensordata`.`time` (`idtime`)"
")ENGINE=InnoDB;")


 
conn = mysql.connector.connect(host='localhost', user='root')
cur = conn.cursor()
    

"""cnx = mysql.connector.connect(user='root')
cursor = cnx.cursor()"""

def create_database(cur):
    try:
        cur.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(dbName))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cur.execute("USE {}".format(dbName))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(dbName))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cur)
        print("Database {} created successfully.".format(dbName))
        conn.database = dbName
    else:
        print(err)
        exit(1)

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cur.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cur.close()
conn.close()

