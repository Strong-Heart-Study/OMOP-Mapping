
/********************************************************************
Name: ETL to populate OMOP Care_Site Table

Source Data tables: SHS Phase123
					OMOP LOCATION 
					OMOP CONCEPT

Destination table: CARE_SITE

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'strongheart.patient' 
		to the appropriate table that contains SHS Phase123 data

	3) Change OMOP_VOCABULARY.vocab51.concept to the 
		appropriate db/table name.

	4) Review the list of available values for place_of_service_concept_id
		values (SQL below) and select the most appropriate one.

Descripton: 	
	
	Currently uses only Variable #9 (CENTER) from the data dictionary
	Other columns are set to null and can be changed if data exists


*********************************************************************/

Use StrongHeart_OMOP
go
	


-- Code to select appropriate place of service concept 
select  distinct * from OMOP_VOCABULARY.vocab51.concept
where domain_id = 'Place of Service'
--currently selected "Ambulatory care site". Please change if needed


 
--create care_site sequence if it does not exist
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'care_site_seq')
	ALTER SEQUENCE dbo.care_site_seq RESTART WITH 1 INCREMENT BY 1  
ELSE 
	CREATE SEQUENCE dbo.care_site_seq  START WITH 1 INCREMENT BY 1;	
;  
GO  


Insert into dbo.care_site(
	care_site_id
	, care_site_name
	, place_of_service_concept_id
	, location_id
	, care_site_source_value
	, place_of_service_source_value	
	)
select next Value for  dbo.care_site_seq as care_site_id 
	, NULL as care_site_name
	, c.concept_id as place_of_service_concept_id
	, l.location_id as location_id
	, a.CENTER as care_site_source_value
	, a.CENTER as place_of_service_source_value
from (select distinct CENTER from strongheart.patient) a
left join dbo.[location] l on a.CENTER = l.location_source_value
left join OMOP_VOCABULARY.vocab51.concept c on c.concept_name = 'Ambulatory care site'  -- please change to most apt description 



