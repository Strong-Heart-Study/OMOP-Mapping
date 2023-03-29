
/********************************************************************
Name: ETL to populate OMOP Location Table

Source Data tables: SHS Phase123

Destination table: LOCATION

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'strongheart.patient' 
		to the appropriate table that contains SHS Phase123 data


Descripton: 	
	
	Currently uses only Variable #9 (CENTER) from the data dictionary
	Other columns are set to null and can be changed if data exists


*********************************************************************/
Use StrongHeart_OMOP
go

	
--create location sequence if it does not exist
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'location_seq')
	ALTER SEQUENCE dbo.location_seq RESTART WITH 1 INCREMENT BY 1  
ELSE 
	CREATE SEQUENCE dbo.location_seq  START WITH 1 INCREMENT BY 1;	
;  
GO  

Insert into dbo.location (
	location_id
	, address_1
	, address_2
	, city
	, state
	, zip
	, county
	, country
	, location_source_value
	, latitude
	, longitude
	)
select next Value for  dbo.location_seq as location_id 
	, NULL as address_1
	, NULL as address_2
	, NULL as city
	, a.CENTER as [state]
	, NULL as zip
	, NULL as county
	, NULL as country
	, a.CENTER as location_source_value
	, NULL as latitude
	, NULL as longitude
from (select distinct CENTER from strongheart.patient) a




