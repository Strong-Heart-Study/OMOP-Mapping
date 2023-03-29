/********************************************************************
Name: ETL to populate OMOP Observation Table

Source Data tables: SHS Phase4
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE

Destination table: Observation

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'omoprawdata.dbo.SHSALL33' 
		to the appropriate table that contains SHS Phase123 data

	3) Change dbo.source_to_concept_map to the 
		appropriate db/table name.

	4) Identify units in the source data for BP (Sys/Dias), 
		BMI and Body surface area. Change the unit_source_value and 
		unit_concept_id accordingly. Use SQL below to identify correct unit
		==============================================================================
			Select * from omop_vocabulary.vocab51.concept where domain_id = 'Unit'
			and standard_concept = 'S'
		==============================================================================

	5) range_low and range_high are currently set to 0. Please update these 
		values if they're available in source data.


*********************************************************************/

use omop1
go


----create Observation sequence if it does not exist
--IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'observation_seq')
--	ALTER SEQUENCE dbo.observation_seq RESTART WITH 1 INCREMENT BY 1  
--ELSE 
--	CREATE SEQUENCE dbo.observation_seq  START WITH 1 INCREMENT BY 1;	
--;  
--GO  


-- Phase IV Waist hip ratio
insert into  [dbo].[observation](
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
      ,[qualifier_source_value]
	  , obs_event_field_concept_id
	  )
select next Value for  dbo.observation_seq as [observation_id]
      , a.[person_id]					as person_id
      ,	a.target_concept_id 			as [observation_concept_id]
      ,	a.observation_date				as [observation_date]
      , a.observation_date				as [observation_datetime]
      , 44818701						as [observation_type_concept_id]  --	From physical examination
      , a.value_as_number				as [value_as_number]
      , NULL							as [qualifier_concept_id]
      , 8523							as [unit_concept_id] --	ratio
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , 'ratio'							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	--phase I Waist hip ratio 
	select pat.[IDNO]						as person_id
	, pat.S4EXDATE							as observation_date
	, 'S4WHR = ' 
		+ cast(S4WHR as varchar(10))		as observation_source_value
	, S4WHR									as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase IV'							as phase
	from omoprawdata.dbo.S4ALL23 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase IV %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S4WHR'
	where pat.S4WHR is not null 
)a


------------------
-- Phase IV Hip Circumference (cm)

insert into  [dbo].[observation](
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
      ,[qualifier_source_value]
	  , obs_event_field_concept_id
	  )
select next Value for  dbo.observation_seq as [observation_id]
      , a.[person_id]					as person_id
      ,	a.target_concept_id 			as [observation_concept_id]
      ,	a.observation_date				as [observation_date]
      , a.observation_date				as [observation_datetime]
      , 44818701						as [observation_type_concept_id]  --	From physical examination
      , a.value_as_number				as [value_as_number]
      , NULL							as [qualifier_concept_id]
      , 8582							as [unit_concept_id] --	cm
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , 'cm'							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	--phase IV hip circumference
	select pat.[IDNO]						as person_id
	, pat.S4EXDATE							as observation_date
	, 'S4HIP = ' 
		+ cast(S4HIP as varchar(10))		as observation_source_value
	, S4HIP									as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase IV'							as phase
	from omoprawdata.dbo.S4ALL23 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase IV %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S4HIP'
	where pat.S4HIP is not null 

)a


---------------------


-- Phase IV Waist Circumference (cm)

insert into  [dbo].[observation](
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
      ,[qualifier_source_value]
	  , obs_event_field_concept_id
	  )
select next Value for  dbo.observation_seq as [observation_id]
      , a.[person_id]					as person_id
      ,	a.target_concept_id 			as [observation_concept_id]
      ,	a.observation_date				as [observation_date]
      , a.observation_date				as [observation_datetime]
      , 44818701						as [observation_type_concept_id]  --	From physical examination
      , a.value_as_number				as [value_as_number]
      , NULL							as [qualifier_concept_id]
      , 8582							as [unit_concept_id] --	cm
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , 'cm'							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	--phase IV  WAIST Circumference 
	select pat.[IDNO]						as person_id
	, pat.S4EXDATE							as observation_date
	, 'S4WAIST = ' 
		+ cast(S4WAIST as varchar(10))		as observation_source_value
	, S4WAIST								as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase IV'							as phase
	from omoprawdata.dbo.S4ALL23 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase IV %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S4WAIST'
	where pat.S4WAIST is not null 

)a

---------------------


-- Phase IV  RIGHT ARM CIRCUMFERENCE (cm)
insert into  [dbo].[observation](
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
      ,[qualifier_source_value]
	  , obs_event_field_concept_id
	  )
select next Value for  dbo.observation_seq as [observation_id]
      , a.[person_id]					as person_id
      ,	a.target_concept_id 			as [observation_concept_id]
      ,	a.observation_date				as [observation_date]
      , a.observation_date				as [observation_datetime]
      , 44818701						as [observation_type_concept_id]  --	From physical examination
      , a.value_as_number				as [value_as_number]
      , NULL							as [qualifier_concept_id]
      , 8582							as [unit_concept_id] --	cm
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , 'cm'							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	--phase IV  right arm circumference (cm)
	select pat.[IDNO]						as person_id
	, pat.S4EXDATE							as observation_date
	, 'EX4_42 = ' 
		+ cast(EX4_42 as varchar(10))		as observation_source_value
	, EX4_42								as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase IV'							as phase
	from omoprawdata.dbo.S4ALL23 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase IV %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'EX4_42'
	where pat.EX4_42 is not null 

)a

---------------------


-- Phase IV  lean body mass (kg)
insert into  [dbo].[observation](
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
      ,[qualifier_source_value]
	  , obs_event_field_concept_id
	  )
select next Value for  dbo.observation_seq as [observation_id]
      , a.[person_id]					as person_id
      ,	a.target_concept_id 			as [observation_concept_id]
      ,	a.observation_date				as [observation_date]
      , a.observation_date				as [observation_datetime]
      , 44818701						as [observation_type_concept_id]  --	From physical examination
      , a.value_as_number				as [value_as_number]
      , NULL							as [qualifier_concept_id]
      , 9529							as [unit_concept_id] --		kilogram
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , 'kg'							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	--phase IV  lean body mass (kg)
	select pat.[IDNO]						as person_id
	, pat.S4EXDATE							as observation_date
	, 'S4LBM = ' 
		+ cast(S4LBM as varchar(10))		as observation_source_value
	, S4LBM									as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase IV'							as phase
	from omoprawdata.dbo.S4ALL23 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase IV %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S4LBM'
	where pat.S4LBM is not null 

)a