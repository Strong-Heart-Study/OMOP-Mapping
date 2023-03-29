/********************************************************************
Name: ETL to populate OMOP Measurement Table

Source Data tables: SHS Phase4
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE

Destination table: Measurement

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'omoprawdata.dbo.SHSALL33' 
		to the appropriate table that contains SHS Phase123 data

	3) Change omop1.dbo.source_to_concept_map to the 
		appropriate db/table name.

	4) Identify units in the source data for Labs. Change the unit_source_value and 
		unit_concept_id accordingly. Use SQL below to identify correct unit
		==============================================================================
			Select * from omop_vocabulary.vocab51.concept where domain_id = 'Unit'
			and standard_concept = 'S'
		==============================================================================

	5) range_low and range_high are currently set to 0. Please update these 
		values if they're available in source data.


*********************************************************************/

	
use omoprawdata 
go




---- SHS education
insert into  omop1.[dbo].[observation](
	   [observation_id]
      ,[person_id]
      ,[observation_concept_id]
      ,[observation_date]
      ,[observation_datetime]
      ,[observation_type_concept_id]
      ,[value_as_number]
      ,[qualifier_concept_id]
      ,[unit_concept_id]
      ,[provider_id]
      ,[visit_occurrence_id]
      ,[observation_source_value]
      ,[observation_source_concept_id]
      ,[unit_source_value]
	  )
select  next Value for  dbo.observation_seq as measurement_id
, a.person_id					as person_id
, a.target_concept_id			as [observation_concept_id]
, a.measurement_date			as [observation_date]
, a.measurement_date			as [observation_datetime]
, 44818704						as [observation_type_concept_id]  --(	Patient reported value) ---select * from OMOP_VOCABULARY.vocab51.CONCEPT where vocabulary_id = 'Meas Type' 
, a.measurement_value			as  value_as_number
, NULL							as [qualifier_concept_id]
, 9448							as unit_concept_id    --		year
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, a.[observation_source_value]	as [observation_source_value]
, 0								as [observation_source_concept_id]
, 'years'						as unit_source_value

from (
	select pat.[IDNO]						as person_id
	, pat.S4EXDATE							as measurement_date
	, 'S4EDU'								as measurement_source_value
	, pat.S4EDU								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	from omoprawdata.dbo.S4ALL23 pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase IV %'

	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= 'S4EDU'
	where pat.S4EDU is not null 

)a
---------------------------
 

