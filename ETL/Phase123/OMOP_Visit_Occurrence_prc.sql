/********************************************************************
Name: ETL to populate OMOP Person Table

Source Data tables: SHS Phase123
					SOURCE_TO_CONCEPT_MAP

Destination table: PERSON

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'strongheart.patient' 
		to the appropriate table that contains SHS Phase123 data

	3) Change OMOP_VOCABULARY.vocab51.source_to_concept_map to the 
		appropriate db/table name.

	4) Review the possible values for visit concept  and 
		and visit_type_concept (SQL below)


*********************************************************************/

	
use strongHeart_OMOP 
go


--Select the appropriate visit  concept 
select * from OMOP_VOCABULARY.vocab51.CONCEPT where vocabulary_id = 'Visit'
	and standard_concept = 'S'
--currently selected "Outpatient Visit". Please change as needed



--Select the appropriate visit type concept 
select * from OMOP_VOCABULARY.vocab51.CONCEPT where  vocabulary_id = 'Visit type'
	and standard_concept = 'S'
--currently selected "Clinical Study visit". Please change as needed


 
--create visit_occurrence sequence if it does not exist
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'visit_occurrence_seq')
	ALTER SEQUENCE dbo.visit_occurrence_seq RESTART WITH 1 INCREMENT BY 1  
ELSE 
	CREATE SEQUENCE dbo.visit_occurrence_seq  START WITH 1 INCREMENT BY 1;	
;  
GO  



---- Insert SHS Phase I visits
Insert into dbo.visit_occurrence (
	visit_occurrence_id
	, person_id
	, visit_concept_id
	, visit_start_date
	, visit_start_datetime
	, visit_end_date
	, visit_end_datetime
	, visit_type_concept_id
	, provider_id
	, care_site_id
	, visit_source_value
	, visit_source_concept_id
	, admitted_from_concept_id
	, admitted_from_source_value
	, discharge_to_source_value
	, discharge_to_concept_id
	, preceding_visit_occurrence_id
)
select next Value for  dbo.visit_occurrence_seq as visit_occurrence_id
, pat.[IDNO] as person_id
, c.concept_id as visit_concept_id
, pat.S1EXDATE as visit_start_date
, pat.S1EXDATE as visit_start_datetime
, pat.S1EXDATE as visit_end_date
, pat.S1EXDATE as visit_end_datetime
, vt.concept_id as visit_type_concept_id 
, NULL as provider_id 
, cs.care_site_id as care_site_id
, 'SHS Phase I ' + cast(pat.S1EXDATE as varchar(20)) as visit_source_value 
, 0 as visit_source_concept_id
, 0 as admitted_from_concept_id
, NULL as admitted_from_source_value
, NULL as discharge_to_source_value
, 0 as discharge_to_concept_id
, 0 as preceding_visit_occurrence_id
from strongheart.patient pat 
left join dbo.care_site cs on cs.care_site_source_value = pat.CENTER
left join OMOP_VOCABULARY.vocab51.CONCEPT c on  c.vocabulary_id = 'Visit'
	and c.concept_name = 'Outpatient Visit'
	and c.standard_concept = 'S'
	
left join OMOP_VOCABULARY.vocab51.CONCEPT vt on vt.vocabulary_id = 'Visit Type'
	and vt.concept_name = 'Clinical Study visit'
	and vt.standard_concept = 'S'
	





---- Insert SHS Phase II visits
Insert into dbo.visit_occurrence (
	visit_occurrence_id
	, person_id
	, visit_concept_id
	, visit_start_date
	, visit_start_datetime
	, visit_end_date
	, visit_end_datetime
	, visit_type_concept_id
	, provider_id
	, care_site_id
	, visit_source_value
	, visit_source_concept_id
	, admitted_from_concept_id
	, admitted_from_source_value
	, discharge_to_source_value
	, discharge_to_concept_id
	, preceding_visit_occurrence_id
)
select next Value for  dbo.visit_occurrence_seq as visit_occurrence_id
, pat.[IDNO] as person_id
, c.concept_id as visit_concept_id
, pat.S2EXDATE as visit_start_date
, pat.S2EXDATE as visit_start_datetime
, pat.S2EXDATE as visit_end_date
, pat.S2EXDATE as visit_end_datetime
, vt.concept_id as visit_type_concept_id 
, NULL as provider_id 
, cs.care_site_id as care_site_id
, 'SHS Phase II ' + cast(pat.S2EXDATE as varchar(20)) as visit_source_value 
, 0 as visit_source_concept_id
, 0 as admitted_from_concept_id
, NULL as admitted_from_source_value
, NULL as discharge_to_source_value
, 0 as discharge_to_concept_id
, vo.visit_occurrence_id as preceding_visit_occurrence_id
from strongheart.patient pat 
left join dbo.care_site cs on cs.care_site_source_value = pat.CENTER
left join OMOP_VOCABULARY.vocab51.CONCEPT c on  c.vocabulary_id = 'Visit'
	and c.concept_name = 'Outpatient Visit'
	and c.standard_concept = 'S'
	
left join OMOP_VOCABULARY.vocab51.CONCEPT vt on vt.vocabulary_id = 'Visit Type'
	and vt.concept_name = 'Clinical Study visit'
	and vt.standard_concept = 'S'
--to find the previous visit 	
left join visit_occurrence vo on vo.person_id = pat.[IDNO] 
	and vo.visit_source_value like 'SHS Phase I%'
where pat.s2exdate is not null 



	
---- Insert SHS Phase III visits
Insert into dbo.visit_occurrence (
	visit_occurrence_id
	, person_id
	, visit_concept_id
	, visit_start_date
	, visit_start_datetime
	, visit_end_date
	, visit_end_datetime
	, visit_type_concept_id
	, provider_id
	, care_site_id
	, visit_source_value
	, visit_source_concept_id
	, admitted_from_concept_id
	, admitted_from_source_value
	, discharge_to_source_value
	, discharge_to_concept_id
	, preceding_visit_occurrence_id
)
select next Value for  dbo.visit_occurrence_seq as visit_occurrence_id
, pat.[IDNO] as person_id
, c.concept_id as visit_concept_id
, pat.S3EXDATE as visit_start_date
, pat.S3EXDATE as visit_start_datetime
, pat.S3EXDATE as visit_end_date
, pat.S3EXDATE as visit_end_datetime
, vt.concept_id as visit_type_concept_id 
, NULL as provider_id 
, cs.care_site_id as care_site_id
, 'SHS Phase III ' + cast(pat.S3EXDATE as varchar(20)) as visit_source_value 
, 0 as visit_source_concept_id
, 0 as admitted_from_concept_id
, NULL as admitted_from_source_value
, NULL as discharge_to_source_value
, 0 as discharge_to_concept_id
, vo.visit_occurrence_id as preceding_visit_occurrence_id
from strongheart.patient pat 
left join dbo.care_site cs on cs.care_site_source_value = pat.CENTER
left join OMOP_VOCABULARY.vocab51.CONCEPT c on  c.vocabulary_id = 'Visit'
	and c.concept_name = 'Outpatient Visit'
	and c.standard_concept = 'S'
	
left join OMOP_VOCABULARY.vocab51.CONCEPT vt on vt.vocabulary_id = 'Visit Type'
	and vt.concept_name = 'Clinical Study visit'
	and vt.standard_concept = 'S'
--to find the previous visit 	
left join visit_occurrence vo on vo.person_id = pat.[IDNO] 
	and vo.visit_source_value like 'SHS Phase II%'
	

where pat.s3exdate is not null 	