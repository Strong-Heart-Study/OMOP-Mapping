/********************************************************************
Name: ETL to populate OMOP Observation Table

Source Data tables: SHS Phase123
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

use omoprawdata
go


--create Observation sequence if it does not exist
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'observation_seq')
	ALTER SEQUENCE dbo.observation_seq RESTART WITH 1 INCREMENT BY 1  
ELSE 
	CREATE SEQUENCE dbo.observation_seq  START WITH 1 INCREMENT BY 1;	
;  
GO  


-- Phase I, II, III Waist hip ratio
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
	, pat.S1EXDATE							as observation_date
	, 'S1WHR = ' 
		+ cast(S1WHR as varchar(10))		as observation_source_value
	, S1WHR									as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1WHR'
	where pat.S1WHR is not null 

	union

	--phase II waist hip ratio
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2WHR = ' 
		+ cast(S2WHR as varchar(10))		as observation_source_value
	, S2WHR									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2WHR'
	where pat.S2WHR is not null 
	
	union

	--phase III waist hip ratio
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3WHR = ' 
		+ cast(S3WHR as varchar(10))		as observation_source_value
	, S3WHR									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3WHR'
	where pat.S3WHR is not null 

)a


------------------
-- Phase I, II, III Hip Circumference (cm)

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
	--phase I hip circumference
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as observation_date
	, 'S1HIP = ' 
		+ cast(S1HIP as varchar(10))		as observation_source_value
	, S1HIP									as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1HIP'
	where pat.S1HIP is not null 

	union

	--phase II hip circumference
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2HIP = ' 
		+ cast(S2HIP as varchar(10))		as observation_source_value
	, S2HIP									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2HIP'
	where pat.S2HIP is not null 
	
	union

	--phase III  hip circumference
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3HIP = ' 
		+ cast(S3HIP as varchar(10))		as observation_source_value
	, S3HIP									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3HIP'
	where pat.S3HIP is not null 

)a


---------------------


-- Phase I, II, III Waist Circumference (cm)

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
	--phase I  WAIST Circumference 
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as observation_date
	, 'S1WAIST = ' 
		+ cast(S1WAIST as varchar(10))		as observation_source_value
	, S1WAIST									as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1WAIST'
	where pat.S1WAIST is not null 

	union

	--phase II waist circumference
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2WAIST = ' 
		+ cast(S2WAIST as varchar(10))		as observation_source_value
	, S2WAIST									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2WAIST'
	where pat.S2WAIST is not null 
	
	union

	--phase III waist circumference
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3WAIST = ' 
		+ cast(S3WAIST as varchar(10))		as observation_source_value
	, S3WAIST									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3WAIST'
	where pat.S3WAIST is not null 

)a
