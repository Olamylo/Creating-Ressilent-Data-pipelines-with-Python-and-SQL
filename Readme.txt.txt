This pipeline is for defining and managing your data efficiently to prevent the breakage of reports using the data.

Data from the API is imported into a staging table.
data is then exported from the staging table to the RAWDATA_TOMTOM_TRAFFIC table, which stores historical data.

Two other tables are then created BM_TOMTOM_TRAFFIC_0 and BM_TOMTOM_TRAFFIC_1.
These two tables serve as a backup mechanism to keep 2 days of data which includes the present day and the previous day. 
The script alternates data extraction from the RAWDATA_TOMTOM_TRAFFIC to either of the BM tables.

Finally, a view is created, this extracts data from the BM table, which has the current-day data.

This mechanism is built to avoid breakage to reports and provide backup.
If the API has an issue or a situation where the data in the staging table is wrong, 
the BM table for the previous day can be utilized till the data in the staging table is restored.


The tables below need to be created in the database, and the datatypes need to be defined accordingly.


CREATE TABLE RAWDATA_TOMTOM_TRAFFIC(
	bulk_id	BIGINT
,	date_added	DATETIME
,	location VARCHAR(200)
,	JamsDelay	FLOAT
,	TrafficIndexLive	INT
,	UpdateTime	DATETIME
,	JamsLength	FLOAT
,	JamsCount	INT
,	TrafficIndexWeekAgo	INT
,	UpdateTimeWeekAgo	DATETIME
) WITH(DATA_COMPRESSION=PAGE)


CREATE TABLE BM_TOMTOM_TRAFFIC_0(
	bulk_id	BIGINT
,	date_added	DATETIME
,	location VARCHAR(200)
,	JamsDelay	FLOAT
,	TrafficIndexLive	INT
,	UpdateTime	DATETIME
,	JamsLength	FLOAT
,	JamsCount	INT
,	TrafficIndexWeekAgo	INT
,	UpdateTimeWeekAgo	DATETIME
) WITH(DATA_COMPRESSION=PAGE)


CREATE TABLE BM_TOMTOM_TRAFFIC_1(
	bulk_id	BIGINT
,	date_added	DATETIME
,	location VARCHAR(200)
,	JamsDelay	FLOAT
,	TrafficIndexLive	INT
,	UpdateTime	DATETIME
,	JamsLength	FLOAT
,	JamsCount	INT
,	TrafficIndexWeekAgo	INT
,	UpdateTimeWeekAgo	DATETIME
) WITH(DATA_COMPRESSION=PAGE)