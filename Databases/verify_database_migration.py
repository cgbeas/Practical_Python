####################################################################################
#                                                                                  #
#   Script name: verify_database_migration.py                                      #
#                                                                                  #
#   This script will verify that the counts from a list of tables i a database     #
#   matches the counts of the same listof tables in a different database.          #
#                                                                                  #
#                                                                                  #
#   Usage:   python verify_database_migration.py conn1 conn2 tables schema         #
#                                                                                  #
#   Maintenance History                                                            #
#                                                                                  #
#   Date        Person            Modification                                     #
#   06/14/2018  Carlos Beas       Initial commit                                   #
#                                                                                  #
####################################################################################


import cx_Oracle
import pandas as pd

def read_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute( query )
        names = [ x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame( rows, columns=names)
    finally:
        if cursor is not None:
            cursor.close()
            
def execute_query(connection, query):
    cursor = connection.cursor() 
    try:
        cursor.execute( query )
    finally:
        if cursor is not None:
            cursor.close()

def verify_database_migration(conn1, conn2, tables, schema):
    verification_table = {}
    db1 = conn1.split('@')[1]
    db2 = conn2.split('@')[1]
    print('Databases to be verified: ', db1, db2)
    
    for table in tables:
        print('Verifying ', table)
        verification_table[table] = {}
        connection = cx_Oracle.connect(conn1)
        q = """SELECT count(*) as count FROM """+schema+"""."""+table
        result_df = read_query(connection=connection, query=q)
        verification_table[table][db1] = result_df['COUNT'].values[0]
        
        connection = cx_Oracle.connect(conn2)
        q = """SELECT count(*) as count FROM """+schema+"""."""+table
        result_df = read_query(connection=connection, query=q)
        verification_table[table][db2] = result_df['COUNT'].values[0]
        
        if(verification_table[table][db1] == verification_table[table][db2]):
            verification_table[table]['counts_match'] = 'TRUE'
        else:
            verification_table[table]['counts_match'] = 'FALSE' 
    
    verification_df = pd.DataFrame(verification_table)
    return verification_df.T


