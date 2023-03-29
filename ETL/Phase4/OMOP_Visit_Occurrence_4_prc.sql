/********************************************************************
Name: ETL to populate OMOP visit_occurrence Table

Source Data tables: SHS Phase123
					SOURCE_TO_CONCEPT_MAP

Destination table: visit_occurrence


*********************************************************************/

	
use omoprawdata 
go


--Select the appropriate visit  concept 
select * from OMOP1.dbo.CONCEPT where vocabulary_id = 'Visit'
	and standard_concept = 'S'
--currently selected "Outpatient Visit". Please change as needed



--Select the appropriate visit type concept 
select * from OMOP1.dbo.CONCEPT where  vocabulary_id = 'Visit type'
	and standard_concept = 'S'
--currently selected "Clinical Study visit". Please change as needed


 



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
, pat.S4EXDATE as visit_start_date
, pat.S4EXDATE as visit_start_datetime
, pat.S4EXDATE as visit_end_date
, pat.S4EXDATE as visit_end_datetime
, vt.concept_id as visit_type_concept_id 
, NULL as provider_id 
, cs.care_site_id as care_site_id
, 'SHS Phase V ' + cast(pat.S4EXDATE as varchar(20)) as visit_source_value 
, 0 as visit_source_concept_id
, 0 as admitted_from_concept_id
, NULL as admitted_from_source_value
, NULL as discharge_to_source_value
, 0 as discharge_to_concept_id
, NULL as preceding_visit_occurrence_id
from omoprawdata.dbo.S4ALL23 pat 
left join omop1.dbo.care_site cs on cs.care_site_source_value = pat.CENTER
left join OMOP1.dbo.CONCEPT c on  c.vocabulary_id = 'Visit'
	and c.concept_name = 'Outpatient Visit'
	and c.standard_concept = 'S'
	
left join OMOP1.dbo.CONCEPT vt on vt.vocabulary_id = 'Visit Type'
	and vt.concept_name = 'Clinical Study visit'
	and vt.standard_concept = 'S'
	
where pat.S4EXDATE is not null 



