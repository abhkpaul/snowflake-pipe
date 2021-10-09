import snowflake.connector


def get_snowflake_conn(userName, password, accountName, warehouseName, databaseName, schemaName):
    conn = snowflake.connector.connect(user=userName,
                                       password=password,
                                       account=accountName,
                                       warehouse=warehouseName,
                                       database=databaseName,
                                       schema=schemaName)
    curs = conn.cursor()
    return curs
