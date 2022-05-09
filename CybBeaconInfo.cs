
WITH 

-- This function will read input stream. primary filteration is done here.
-- Best practice: try to read stream from one function only instead of multiple.
IncomingDataQ AS (    
    SELECT 
        [timestamp]
        ,[dataformat]
        ,[blemac(hex)]
        ,[rssi(dbm)]
        ,[ibeaconuuid(hex)]
        ,[ibeaconmajor]
        ,[ibeaconminor]
        ,[rssi@1m(dbm)]
        ,[namespace(hex)]
        ,[instanceid(hex)]
        ,[rssi@0m(dbm)]
        ,[isDuress]
		,GetMetadataPropertyValue(EdgeHubInput, 'EdgeHub.ConnectionDeviceId') AS BeaconGatewayId		 
    FROM EdgeHubInput 
    WHERE 
    ([blemac(hex)] <> '')  
    --AND 
    --((lower(dataformat) LIKE '%ibeacon%') OR (lower(dataformat) LIKE '%eddystone%'))
    -- Filter using RSSI 
    --AND [rssi(dbm)] > -80
)

-- Tranform all data into same format.
-- Remove unneccary column and put all generalized data in single.
-- Currently supported for ibeacon and eddystone-UID only.
-- Add case condition to support more.
,DataTansformQ AS (
    SELECT 
	    [blemac(hex)] AS blemac
	    ,[rssi(dbm)] AS rssi_measured
	    ,[dataformat]
	    ,TRY_CAST ([timestamp] AS datetime) SLAtimestamp
	    ,[BeaconGatewayId]        
        ,[isDuress]
        ,System.Timestamp() as Systemtimestamp
	    ,CASE
			WHEN lower(dataformat) LIKE '%ibeacon%' THEN [rssi@1m(dbm)]
			WHEN lower(dataformat) LIKE '%eddystone%' THEN [rssi@0m(dbm)] - 41
			ELSE 0
		END AS rssi_1meter,
		CASE
			WHEN lower(dataformat) LIKE '%ibeacon%' THEN concat([ibeaconuuid(hex)] , ',' , ibeaconmajor , ',' , ibeaconminor)
			WHEN lower(dataformat) LIKE '%eddystone%' THEN concat([namespace(hex)] , ',' , [instanceid(hex)])
			ELSE NULL
		END AS beaconids
	FROM IncomingDataQ 
)

,DuressFilter as (
SELECT 
streamData.BLEMAC as BeaconId
,streamData.SLAtimestamp as SLAtimestamp
,System.Timestamp()  [timestamp]
,streamData.isDuress 

FROM DataTansformQ streamData 
where streamData.isDuress = 1
)
-- Aggregate Last 5 second data every 1 second
-- and get only latest value of rssi per Beacon-BeaconGateway in window 
-- it also filters duplicate values as it group them as 1 in collectTop
-- Make sure to take latest value of duplicate recived . ex: order by streamData.Systemtimestamp streamData.Timestamp desc
,BoundDataQ AS (
SELECT 
streamData.BLEMAC
,streamData.BEACONIDS
,streamData.[BeaconGatewayId]
,MAX(streamData.SLAtimestamp) as SLAtimestamp
,System.Timestamp()  sysTimestamp
,collecttop(1) over (order by streamData.Systemtimestamp desc) as valuearray
FROM DataTansformQ streamData 

Group by 
hopping(second,5,1)
,streamData.blemac
,streamData.beaconids
,streamData.[BeaconGatewayId]
)

,ReadFromRank AS (
SELECT 
streamData.BLEMAC
,streamData.[BeaconGatewayId]
,streamData.sysTimestamp as TimeStamp 
,streamData.SLAtimestamp 
,GetArrayElement(streamData.valuearray,0).value.rssi_1meter AS RSSI_1Meter
,GetArrayElement(streamData.valuearray,0).value.rssi_measured AS AvgRSSI
,RefLocation.tags.Location
FROM BoundDataQ as streamData 
JOIN RefData RefLocation
ON RefLocation.deviceId = streamData.BeaconGatewayId
)

-- NOTE: Create structre of Reff data (Create Table), so it will be easy to read
-- Filter data for whom x,y cordinate are empty. 

-- Send one array containing all data neccesary for calculation in UDF function. to maintainn it's stateless property.
-- Make sure no duplicates are here.
,ConvertToArray AS (
SELECT 
    streamData.BLEMAC
    ,streamData.TimeStamp
    ,MAX(streamData.SLAtimestamp) [SLAtimestamp]
    ,Collect() AS Map 
FROM ReadFromRank streamData 
GROUP BY streamData.BLEMAC, streamData.TimeStamp,System.Timestamp()
)

,CalculateLocation as(
SELECT 
    BLEMAC as BeaconId
    ,System.Timestamp [timestamp]
    ,SLAtimestamp
    ,UDF.ETTelemetry_StandaloneMultiLateration_Calculate(Map) [Location]
    FROM ConvertToArray
)



-- calculated location to singalR for displaying

SELECT 
     streamData.BeaconId
    ,streamData.[timestamp]
    ,streamData.[Location]
    ,streamData.SLAtimestamp
--    , TRY_CAST(streamData.[Location].XCor AS Float) Xcor
--    , TRY_CAST(streamData.[Location].YCor AS Float) Ycor
--     ,udf.ETTelemetry_TimeDiffCalculation_showDiff(streamData.[timestamp],streamData.SLAtimestamp) as Diff
INTO ToCloud 
FROM CalculateLocation streamData
WHERE streamData.Location IS NOT NULL

--select * Into DuressOutput
--FROM DuressFilter




WITH 

-- This function will read input stream. primary filteration is done here.
-- Best practice: try to read stream from one function only instead of multiple.
IncomingDataQ AS (    
    SELECT 
        [timestamp]
        ,[dataformat]
        ,[blemac(hex)]
        ,[rssi(dbm)]
        ,[ibeaconuuid(hex)]
        ,[ibeaconmajor]
        ,[ibeaconminor]
        ,[rssi@1m(dbm)]
        ,[namespace(hex)]
        ,[instanceid(hex)]
        ,[rssi@0m(dbm)]
        ,[isDuress]
		,GetMetadataPropertyValue(EdgeHubInput, 'EdgeHub.ConnectionDeviceId') AS BeaconGatewayId		 
    FROM EdgeHubInput 
    WHERE 
    ([blemac(hex)] <> '')  
    --AND 
    --((lower(dataformat) LIKE '%ibeacon%') OR (lower(dataformat) LIKE '%eddystone%'))
    -- Filter using RSSI 
    --AND [rssi(dbm)] > -80
)

-- Tranform all data into same format.
-- Remove unneccary column and put all generalized data in single.
-- Currently supported for ibeacon and eddystone-UID only.
-- Add case condition to support more.
,DataTansformQ AS (
    SELECT 
	    [blemac(hex)] AS blemac
	    ,[rssi(dbm)] AS rssi_measured
	    ,[dataformat]
	    ,TRY_CAST ([timestamp] AS datetime) SLAtimestamp
	    ,[BeaconGatewayId]        
        ,[isDuress]
        ,System.Timestamp() as Systemtimestamp
	    ,CASE
			WHEN lower(dataformat) LIKE '%ibeacon%' THEN [rssi@1m(dbm)]
			WHEN lower(dataformat) LIKE '%eddystone%' THEN [rssi@0m(dbm)] - 41
			ELSE 0
		END AS rssi_1meter,
		CASE
			WHEN lower(dataformat) LIKE '%ibeacon%' THEN concat([ibeaconuuid(hex)] , ',' , ibeaconmajor , ',' , ibeaconminor)
			WHEN lower(dataformat) LIKE '%eddystone%' THEN concat([namespace(hex)] , ',' , [instanceid(hex)])
			ELSE NULL
		END AS beaconids
	FROM IncomingDataQ 
)

,DuressFilter as (
SELECT 
streamData.BLEMAC as BeaconId
,streamData.SLAtimestamp as SLAtimestamp
,System.Timestamp()  [timestamp]
,streamData.isDuress 

FROM DataTansformQ streamData 
where streamData.isDuress = 1
)
-- Aggregate Last 5 second data every 1 second
-- and get only latest value of rssi per Beacon-BeaconGateway in window 
-- it also filters duplicate values as it group them as 1 in collectTop
-- Make sure to take latest value of duplicate recived . ex: order by streamData.Systemtimestamp streamData.Timestamp desc
,BoundDataQ AS (
SELECT 
streamData.BLEMAC
,streamData.BEACONIDS
,streamData.[BeaconGatewayId]
,MAX(streamData.SLAtimestamp) as SLAtimestamp
,System.Timestamp()  sysTimestamp
,collecttop(1) over (order by streamData.Systemtimestamp desc) as valuearray
FROM DataTansformQ streamData 

Group by 
hopping(second,5,1)
,streamData.blemac
,streamData.beaconids
,streamData.[BeaconGatewayId]
)

,ReadFromRank AS (
SELECT 
streamData.BLEMAC
,streamData.[BeaconGatewayId]
,streamData.sysTimestamp as TimeStamp 
,streamData.SLAtimestamp 
,GetArrayElement(streamData.valuearray,0).value.rssi_1meter AS RSSI_1Meter
,GetArrayElement(streamData.valuearray,0).value.rssi_measured AS AvgRSSI
,RefLocation.tags.Location
FROM BoundDataQ as streamData 
JOIN RefData RefLocation
ON RefLocation.deviceId = streamData.BeaconGatewayId
)

-- NOTE: Create structre of Reff data (Create Table), so it will be easy to read
-- Filter data for whom x,y cordinate are empty. 

-- Send one array containing all data neccesary for calculation in UDF function. to maintainn it's stateless property.
-- Make sure no duplicates are here.
,ConvertToArray AS (
SELECT 
    streamData.BLEMAC
    ,streamData.TimeStamp
    ,MAX(streamData.SLAtimestamp) [SLAtimestamp]
    ,Collect() AS Map 
FROM ReadFromRank streamData 
GROUP BY streamData.BLEMAC, streamData.TimeStamp,System.Timestamp()
)

,CalculateLocation as(
SELECT 
    BLEMAC as BeaconId
    ,System.Timestamp [timestamp]
    ,SLAtimestamp
    ,UDF.ETTelemetry_StandaloneMultiLateration_Calculate(Map) [Location]
    FROM ConvertToArray
)



-- calculated location to singalR for displaying

SELECT 
     streamData.BeaconId
    ,streamData.[timestamp]
    ,streamData.[Location]
    ,streamData.SLAtimestamp
--    , TRY_CAST(streamData.[Location].XCor AS Float) Xcor
--    , TRY_CAST(streamData.[Location].YCor AS Float) Ycor
--     ,udf.ETTelemetry_TimeDiffCalculation_showDiff(streamData.[timestamp],streamData.SLAtimestamp) as Diff
INTO ToCloud 
FROM CalculateLocation streamData
WHERE streamData.Location IS NOT NULL

--select * Into DuressOutput
--FROM DuressFilter

WITH 

-- This function will read input stream. primary filteration is done here.
-- Best practice: try to read stream from one function only instead of multiple.
IncomingDataQ AS (    
    SELECT 
        [timestamp]
        ,[dataformat]
        ,[blemac(hex)]
        ,[rssi(dbm)]
        ,[ibeaconuuid(hex)]
        ,[ibeaconmajor]
        ,[ibeaconminor]
        ,[rssi@1m(dbm)]
        ,[namespace(hex)]
        ,[instanceid(hex)]
        ,[rssi@0m(dbm)]
        ,[isDuress]
		,GetMetadataPropertyValue(EdgeHubInput, 'EdgeHub.ConnectionDeviceId') AS BeaconGatewayId		 
    FROM EdgeHubInput 
    WHERE 
    ([blemac(hex)] <> '')  
    --AND 
    --((lower(dataformat) LIKE '%ibeacon%') OR (lower(dataformat) LIKE '%eddystone%'))
    -- Filter using RSSI 
    --AND [rssi(dbm)] > -80
)

-- Tranform all data into same format.
-- Remove unneccary column and put all generalized data in single.
-- Currently supported for ibeacon and eddystone-UID only.
-- Add case condition to support more.
,DataTansformQ AS (
    SELECT 
	    [blemac(hex)] AS blemac
	    ,[rssi(dbm)] AS rssi_measured
	    ,[dataformat]
	    ,TRY_CAST ([timestamp] AS datetime) SLAtimestamp
	    ,[BeaconGatewayId]        
        ,[isDuress]
        ,System.Timestamp() as Systemtimestamp
	    ,CASE
			WHEN lower(dataformat) LIKE '%ibeacon%' THEN [rssi@1m(dbm)]
			WHEN lower(dataformat) LIKE '%eddystone%' THEN [rssi@0m(dbm)] - 41
			ELSE 0
		END AS rssi_1meter,
		CASE
			WHEN lower(dataformat) LIKE '%ibeacon%' THEN concat([ibeaconuuid(hex)] , ',' , ibeaconmajor , ',' , ibeaconminor)
			WHEN lower(dataformat) LIKE '%eddystone%' THEN concat([namespace(hex)] , ',' , [instanceid(hex)])
			ELSE NULL
		END AS beaconids
	FROM IncomingDataQ 
)

,DuressFilter as (
SELECT 
streamData.BLEMAC as BeaconId
,streamData.SLAtimestamp as SLAtimestamp
,System.Timestamp()  [timestamp]
,streamData.isDuress 

FROM DataTansformQ streamData 
where streamData.isDuress = 1
)
-- Aggregate Last 5 second data every 1 second
-- and get only latest value of rssi per Beacon-BeaconGateway in window 
-- it also filters duplicate values as it group them as 1 in collectTop
-- Make sure to take latest value of duplicate recived . ex: order by streamData.Systemtimestamp streamData.Timestamp desc
,BoundDataQ AS (
SELECT 
streamData.BLEMAC
,streamData.BEACONIDS
,streamData.[BeaconGatewayId]
,MAX(streamData.SLAtimestamp) as SLAtimestamp
,System.Timestamp()  sysTimestamp
,collecttop(1) over (order by streamData.Systemtimestamp desc) as valuearray
FROM DataTansformQ streamData 

Group by 
hopping(second,5,1)
,streamData.blemac
,streamData.beaconids
,streamData.[BeaconGatewayId]
)

,ReadFromRank AS (
SELECT 
streamData.BLEMAC
,streamData.[BeaconGatewayId]
,streamData.sysTimestamp as TimeStamp 
,streamData.SLAtimestamp 
,GetArrayElement(streamData.valuearray,0).value.rssi_1meter AS RSSI_1Meter
,GetArrayElement(streamData.valuearray,0).value.rssi_measured AS AvgRSSI
,RefLocation.tags.Location
FROM BoundDataQ as streamData 
JOIN RefData RefLocation
ON RefLocation.deviceId = streamData.BeaconGatewayId
)

-- NOTE: Create structre of Reff data (Create Table), so it will be easy to read
-- Filter data for whom x,y cordinate are empty. 

-- Send one array containing all data neccesary for calculation in UDF function. to maintainn it's stateless property.
-- Make sure no duplicates are here.
,ConvertToArray AS (
SELECT 
    streamData.BLEMAC
    ,streamData.TimeStamp
    ,MAX(streamData.SLAtimestamp) [SLAtimestamp]
    ,Collect() AS Map 
FROM ReadFromRank streamData 
GROUP BY streamData.BLEMAC, streamData.TimeStamp,System.Timestamp()
)

,CalculateLocation as(
SELECT 
    BLEMAC as BeaconId
    ,System.Timestamp [timestamp]
    ,SLAtimestamp
    ,UDF.ETTelemetry_StandaloneMultiLateration_Calculate(Map) [Location]
    FROM ConvertToArray
)



-- calculated location to singalR for displaying

SELECT 
     streamData.BeaconId
    ,streamData.[timestamp]
    ,streamData.[Location]
    ,streamData.SLAtimestamp
--    , TRY_CAST(streamData.[Location].XCor AS Float) Xcor
--    , TRY_CAST(streamData.[Location].YCor AS Float) Ycor
--     ,udf.ETTelemetry_TimeDiffCalculation_showDiff(streamData.[timestamp],streamData.SLAtimestamp) as Diff
INTO ToCloud 
FROM CalculateLocation streamData
WHERE streamData.Location IS NOT NULL

--select * Into DuressOutput
--FROM DuressFilter