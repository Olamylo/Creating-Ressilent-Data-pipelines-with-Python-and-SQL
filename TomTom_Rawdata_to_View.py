from sqlalchemy import create_engine

engine = create_engine('mssql+pyodbc://OLAMYLO\SQLEXPRESS/Mylo_DB?driver=SQL+Server+Native+Client+11.0')


def try_except_execute(sqlCmd):
    '''
    try except to execute sqlcmd command line
    '''
    try:
        with engine.begin() as conn:
            conn.execute(sqlCmd)
    except Exception as e:
        raise Exception("Error in pushing data to SQL", e)


def get_bm_table_name(table_name):
    '''
    Get the current BM table name to be used
    '''
    # Need to fetch currently used BM table for the given view and switch it around
    #################################################################################
    cmdGetCurrBMTableName = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.view_table_usage WITH(NOLOCK)
	                        WHERE view_name = 'v_bm_{table_name}'
                            """
    dbCurrTbl = ''
    # Connect to DB and get BM table
    with engine.begin() as conn:
        rs = engine.execute(cmdGetCurrBMTableName).fetchone()

        if rs is None:
            raise "No result found for table"
        else:
            # If table returned ends with _0 we will push data to _1 and vice versa.
            # This is to ensure smooth operation in terms of data push.
            print(f'Previous BM table used: {rs[0]}')
            dbCurrTbl = f"BM_{table_name}_0" if str(rs[0])[-1] == "1" else f"BM_{table_name}_1"
            print(f'Current BM table used: {dbCurrTbl}')
            # Truncate the BM table that is going to be used
        conn.execute("TRUNCATE TABLE {}".format(dbCurrTbl))
    return dbCurrTbl


def rawdata_to_bm(dbCurrTbl, table_name):
    '''
    Insert latest bulk_id data for each machines into BM table
    '''
    # Merge tables for pushing latest data to BM_0/BM_1 table
    #################################################################################
    cmdTable = f"""INSERT INTO [{dbCurrTbl}] SELECT * FROM [RAWDATA_{table_name}] WHERE bulk_id = (SELECT MAX(bulk_id) FROM RAWDATA_{table_name} WITH(NOLOCK))"""
    # Insert into tables
    try_except_execute(cmdTable)
    print(f'BM table {dbCurrTbl} has been updated.')


def bm_to_view(dbCurrTbl, table_name):
    '''
    Step to switch view upon successful data pushed to BM
    '''
    cmdToView = f"""DECLARE @stmtCols AS NVARCHAR(MAX)  = (select STRING_AGG(CONCAT('[',col.name,']'),',') from sys.columns col with(nolock)
                    WHERE NOT col.name = 'bulk_id' 
                    AND NOT col.Name = 'SourceType' AND col.object_Id = OBJECT_ID('{dbCurrTbl}'))

                    DECLARE @stmtQuery AS NVARCHAR(MAX) = ('ALTER VIEW v_bm_{table_name} AS SELECT '+ @stmtCols +' FROM {dbCurrTbl} WITH(NOLOCK)');
                    EXEC sp_executesql @stmtQuery"""
    try_except_execute(cmdToView)
    print(f'View v_bm_{table_name} has been altered.')


def raw_to_view(table_name):
    print(f'======== {table_name} ========')
    dbCurrTbl = get_bm_table_name(table_name)
    rawdata_to_bm(dbCurrTbl, table_name)
    bm_to_view(dbCurrTbl, table_name)


def main():
    #Getting list of rawdata tables.
    cmdSelect = f"""SELECT NAME FROM sys.tables WITH(NOLOCK) WHERE NAME LIKE 'RAWDATA_TOMTOM_%'"""

    print('Getting list of tables')
    with engine.begin() as conn:
        rs = conn.execute(cmdSelect)
        fetch_lst = rs.fetchall()
    print(fetch_lst)
    [raw_to_view(row[0].replace('RAWDATA_', '')) for row in fetch_lst]

    # raw_to_view('RAWDATA_TOMTOM_TRAFFIC')


if __name__ == "__main__":
    main()