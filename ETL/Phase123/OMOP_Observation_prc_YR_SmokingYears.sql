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

		--observation type concept id (possible options)
		44814721	Patient reported	Type Concept	Observation Type	Observation Type
		45754907	Derived value	Type Concept	Meas Type	Meas Type
		44818701	From physical examination	Type Concept	Meas Type	Meas Type

		--Observatin value concept ids (possible values)
		4188539	Yes	Meas Value	SNOMED
		4188540	No	Meas Value	SNOMED	Qualifier Value
		4125554	Impaired	Meas Value	SNOMED	Qualifier Value
		4128331	Pregnancy	Observation	SNOMED	Qualifier Value
*********************************************************************/

use omoprawdata
go




-- Unpivot the source table. 
if object_id('tempdb.dbo.#obs') is not null drop table #obs
SELECT IDNO, obs_name, obs_value, 
	cast('' as varchar(10)) obs_unit,
	0 obs_unit_concept_id,
	cast('' as date) as obs_date,  
	case when left(obs_name,2) ='S1' then 'Phase I'
	when left(obs_name,2) = 'S2' then 'Phase II'
	when left(obs_name,2) = 'S3' then 'Phase III' end as Phase
into #obs
FROM   
   (SELECT [IDNO]
	, S1ADADMD
	, S1PPY
	, S1SMKD

	, S2ADADMD
	, S2PPY
	, S2SMKD

	, S3ADADMD
	, S3PPY
	, S3SMKD

   FROM [omoprawdata].dbo.SHSALL33 ) p  
UNPIVOT  
   (obs_value FOR obs_name IN   
      (	  
		S1ADADMD
	, S1PPY
	, S1SMKD

	, S2ADADMD
	, S2PPY
	, S2SMKD

	, S3ADADMD
	, S3PPY
	, S3SMKD
	  )  
)AS unpvt;  
GO



--add the measurement date
update #obs set obs_date = case 
	when obs_name like '_1%' then rw.S1EXDATE 
	when obs_name like '_2%' then rw.S2EXDATE 
	when obs_name like '_3%' then rw.S3EXDATE 
end
, obs_unit = case 
	when  substring(obs_name, 3, len(obs_name)-2) in ('ADADMD','SMKD') THEN 'years'
	else NULL 
end
,  obs_unit_concept_id = case 
	when  substring(obs_name, 3, len(obs_name)-2) in ('ADADMD','SMKD') THEN 9448
	else 0 
end
 from #obs  mg
join [omoprawdata].dbo.[SHSALL33]rw on rw.idno = mg.idno 



select * from #obs 


-- Phase123
insert into  omop1.[dbo].[observation](
	   [observation_id]
      ,[person_id]
      ,[observation_concept_id]
      ,[observation_date]
      ,[observation_datetime]
      ,[observation_type_concept_id]
	  ,value_as_number
      ,[value_as_concept_id]
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
      , 44814721						as [observation_type_concept_id]  --		Patient reported
	  , a.value_as_number				as value_as_number
      , a.[value_as_concept_id]			as [value_as_concept_id]
      , unit_concept_id					as [unit_concept_id] 
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , unit							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	select pat.[IDNO]						as person_id
	, pat.obs_date							as observation_date
	, obs_name + ' = '
		+ cast(obs_value as varchar(10))	as observation_source_value
	, obs_value								as value_as_number
	, NULL									as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, obs_unit								as unit 
	, obs_unit_concept_id					as unit_concept_id
	, 'Phase I'								as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.obs_name
	where pat.obs_name like 'S1%'

	union 

	select pat.[IDNO]						as person_id
	, pat.obs_date							as observation_date
	, obs_name + ' = '
		+ cast(obs_value as varchar(10))	as observation_source_value
	, obs_value								as value_as_number
	, NULL									as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, obs_unit								as unit 
	, obs_unit_concept_id					as unit_concept_id
	, 'Phase II'							as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase II %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.obs_name
	where pat.obs_name like 'S2%'

	union 

	select pat.[IDNO]						as person_id
	, pat.obs_date							as observation_date
	, obs_name + ' = '
		+ cast(obs_value as varchar(10))	as observation_source_value
	, obs_value								as value_as_number
	, NULL									as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, obs_unit								as unit 
	, obs_unit_concept_id					as unit_concept_id
	, 'Phase III'							as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase III %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.obs_name
	where pat.obs_name like 'S3%'


)a
