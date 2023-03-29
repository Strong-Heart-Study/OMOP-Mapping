/********************************************************************
Name: ETL to populate OMOP visit_occurrence Table

Source Data tables: SHS Phase123
					SOURCE_TO_CONCEPT_MAP

Destination table: visit_occurrence


*********************************************************************/
select * from omop1.dbo.concept 
where concept_id in (
	32883	--SURVEY
	,32865	--PATIENT SELF-REPORT
	,32862	--PATIENT FILLED SURVEY
	,32851  --Healthcare professional filled survey
)	


use omoprawdata 
go


---- Insert SHS Phase V visits
Insert into omop1.dbo.visit_occurrence (
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
, pat.S6EXDATE as visit_start_date
, pat.S6EXDATE as visit_start_datetime
, pat.S6EXDATE as visit_end_date
, pat.S6EXDATE as visit_end_datetime
, 32851 as visit_type_concept_id 
, NULL as provider_id 
, cs.care_site_id as care_site_id
, 'SHS Phase VI ' + cast(pat.S6EXDATE as varchar(20)) as visit_source_value 
, 0 as visit_source_concept_id
, 0 as admitted_from_concept_id
, NULL as admitted_from_source_value
, NULL as discharge_to_source_value
, 0 as discharge_to_concept_id
, NULL as preceding_visit_occurrence_id
from omoprawdata.dbo.s6all pat 
left join omop1.dbo.care_site cs on cs.care_site_source_value = pat.CENTER
left join OMOP1.dbo.CONCEPT c on  c.vocabulary_id = 'Visit'
	and c.concept_name = 'Outpatient Visit'
	and c.standard_concept = 'S'
	
left join OMOP1.dbo.CONCEPT vt on vt.vocabulary_id = 'Visit Type'
	and vt.concept_name = 'Clinical Study visit'
	and vt.standard_concept = 'S'
	
where pat.S6EXDATE is not null 



-- Update non-standard visit_type_concept_ids
update omop1.dbo.visit_occurrence 
set visit_type_concept_id = 32851	--Healthcare professional filled survey
where visit_type_concept_id != 32851

begin transaction t1
	update omop1.dbo.visit_occurrence 
	set visit_start_date = convert(datetime, visit_start_date, 121)
	, visit_start_datetime = convert(datetime, visit_start_datetime, 121)
	, visit_end_date = convert(datetime, visit_end_date, 121)
	, visit_end_datetime = convert(datetime, visit_end_datetime, 121)


--commit transaction t1