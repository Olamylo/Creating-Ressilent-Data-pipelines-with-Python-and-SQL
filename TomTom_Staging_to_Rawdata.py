from sqlalchemy import create_engine

engine = create_engine('mssql+pyodbc://servername/databasename?driver=SQL+Server+Native+Client+11.0')

def stage_to_raw(stageTable, rawTable):
    ''' Function to insert data from Staging into RAWDATA after checking on the latest bulk_id with respect to each data source.
    '''

    # Check if there are existing data being pushed before, make sure no duplicate bulk_id being insert to Rawdata
    # if return 0 count means no existing bulk_id, if some values means need to check is it same count as latest Staging data
    # (highly unlikely have different counts)
    cmdExistRawCount = f"""SELECT count(*) from {rawTable} WITH(NOLOCK) 
            where bulk_id = (select max(bulk_id) from {stageTable} WITH(NOLOCK))"""

    # Get the count of rows in the latest staging table
    cmdLatestStageCount = f"""SELECT COUNT(*) from {stageTable} WITH(NOLOCK)"""

    # Insert into Rawdata with more dynamic query
    cmdTransfer = f"""DECLARE @stmtRawCols AS NVARCHAR(MAX) = (SELECT STRING_AGG(CONCAT('[',COLUMN_NAME,']'),',') FROM INFORMATION_SCHEMA.COLUMNS WITH(NOLOCK)
                                    WHERE  TABLE_NAME = '{rawTable}')

                        DECLARE @stmtStageCols AS NVARCHAR(MAX)  = (SELECT 
                                    STRING_AGG(
                                    CASE WHEN DATA_TYPE = 'uniqueidentifier' THEN
                                    CONCAT('IIF(LEN(',COLUMN_NAME,') > 0 ,CAST(', COLUMN_NAME ,' AS UNIQUEIDENTIFIER),NULL)')
                                    WHEN DATA_TYPE = 'varbinary' THEN
                                    CONCAT('IIF(LEN(',COLUMN_NAME,') > 0 ,CONVERT(VARBINARY(85), ', COLUMN_NAME ,',1),NULL)')
                                    WHEN DATA_TYPE = 'bit' THEN
                                    CONCAT('IIF(LEN(',COLUMN_NAME,') > 0 ,CAST(', COLUMN_NAME ,' AS BIT),NULL)')
                                    ELSE
                                    CONCAT('[',COLUMN_NAME,']')
                                    END 
                                    ,',')
                                    FROM INFORMATION_SCHEMA.COLUMNS WITH(NOLOCK)
                                    WHERE  TABLE_NAME = '{rawTable}')

                        DECLARE @stmtQuery AS NVARCHAR(MAX) = ('INSERT INTO [{rawTable}] ('+ @stmtRawCols +') 
                                SELECT '+ @stmtStageCols +' FROM {stageTable} WITH(NOLOCK)');
                    EXEC sp_executesql @stmtQuery"""

    with engine.begin() as conn:
        raw_count = conn.execute(cmdExistRawCount).fetchone()
        if raw_count[0] == 0:
            print(f"Transferring data from {stageTable}")
            try:
                conn.execute(cmdTransfer)
            except Exception as e:
                print(f"Error in pushing data to SQL, {e}")
                import sys
                sys.exit(1)
        else:
            stage_count = conn.execute(cmdLatestStageCount).fetchone()
            if raw_count[0] == stage_count[0]:
                print(f"Rawdata table: {rawTable} already has the latest data from Staging table: {stageTable}.")
            else:
                raise Exception(
                    f"Rawdata table: {rawTable} rows count and Staging table: {stageTable} rows count not match! PLEASE INVESTIGATE!")

def main():

# Getting list of RAWDATA tables.
    cmdSelect = f"""SELECT NAME FROM sys.tables WITH(NOLOCK) WHERE NAME LIKE 'RAWDATA_TOMTOM_%'"""

    print('Getting list of tables')
    with engine.begin() as conn:
        rs = conn.execute(cmdSelect)
        fetch_lst = rs.fetchall()
    [stage_to_raw(f'STAGING_{row[0].replace("RAWDATA_","")}', row[0]) for row in fetch_lst]

# stage_to_raw('STAGING_TOMTOM_TRAFFIC', 'RAWDATA_TOMTOM_TRAFFIC')

if __name__ == "__main__":
    main()
