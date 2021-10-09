from sf_conn import get_snowflake_conn
from jproperties import Properties

def from_csv_to_sf(cur, sqlstmt):
    for sqllist in sqlstmt:
        for sql in sqllist:
            print("Executing SQL : ", sql)
            try:
                cur.execute(sql)
            except Exception as e:
                print("Load Failed.", str(e))
        print("Load Success.")
    cur.close()


if __name__ == "__main__":

    configs = Properties()
    with open('app-config.properties', 'rb') as config_file:
        configs.load(config_file)

    sf_user = configs.get("sf_user").data
    sf_pass = configs.get("sf_pass").data
    sf_account = configs.get("sf_account").data
    sf_warehouse = configs.get("sf_warehouse").data
    sf_database = configs.get("sf_database").data
    sf_schema = configs.get("sf_schema").data
    source_csv = configs.get("csv_location").data
    stage_name = configs.get("sf_stage_name").data
    pipe_name = configs.get("sf_stage_name").data

    cursr = get_snowflake_conn(userName=sf_user,
                               password=sf_pass,
                               accountName=sf_account,
                               warehouseName=sf_warehouse,
                               databaseName=sf_database,
                               schemaName=sf_schema)

    sql_stmts = [[
        "PUT file://" + source_csv + " @" + stage_name + " auto_compress=true",
        "alter pipe " + pipe_name + " refresh"
    ]]

    from_csv_to_sf(cursr, sql_stmts)
