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
	cast(NULL as datetime) as obs_date,  
	cast(NULL as varchar(10)) as Phase,
	cast(NULL as varchar(50)) as value_as_string
into #obs
FROM   
   (SELECT [IDNO]
	, S1INCOME
	, S2INCOME
	, S3INCOME
   FROM [omoprawdata].dbo.SHSALL33 ) p  
UNPIVOT  
   (obs_value FOR obs_name IN   
      (	  
	  S1INCOME
	, S2INCOME
	, S3INCOME
	  )  
)AS unpvt;  
GO


select * from #obs 

/***********************************************************
need to map the values in source DB to acutal string values 
***********************************************************/
--add the Observation date and units 
update mg set obs_date = case 
		when obs_name like '_1%' then  rw.S1EXDATE 
		when obs_name like '_2%' then  rw.S2EXDATE
		when obs_name like '_3%' then  rw.S3EXDATE 
	end
, Phase = case 
		when obs_name like '_1%' then  'Phase I' 
		when obs_name like '_2%' then  'Phase II' 
		when obs_name like '_3%' then  'Phase III' 
	end
, value_as_string = case 
		when obs_value = '1' then '<5000'
		when obs_value = '2' then '5001 - 10000' 
		when obs_value = '3' then '10001 - 15000' 
		when obs_value = '4' then '15001 - 20000'

		when obs_value = '5' then '20001 - 25000' 
		when obs_value = '6' then '25001 - 35000' 
		when obs_value = '7' then '35001 - 50000' 
		when obs_value = '8' then 'over 50000'

		when obs_value = '9' then 'Don''t know/not sure'
		when obs_value = '0' then 'Refused'

	end
 from #obs mg
join [omoprawdata].dbo.SHSALL33 rw on rw.idno = mg.idno 





select * from #obs


-- Phase I, II, III
insert into  omop1.[dbo].[observation](
	   [observation_id]
      ,[person_id]
      ,[observation_concept_id]
      ,[observation_date]
      ,[observation_datetime]
      ,[observation_type_concept_id]
	  ,[value_as_string]
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
	  , a.value_as_string				as [value_as_string]
      , a.[value_as_concept_id]			as [value_as_concept_id]
      , 0								as [unit_concept_id] 
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , NULL							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	

	select pat.[IDNO]						as person_id
	, pat.obs_date							as observation_date
	, obs_name + ' = '
		+ obs_value							as observation_source_value
	, obs_value								as [value_as_string]
	, NULL									as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.obs_name
	where pat.obs_name like 'S1%'

	union

	--phase II 
	select pat.[IDNO]						as person_id
	, pat.obs_date							as observation_date
	, obs_name + ' = '
		+ obs_value							as observation_source_value
	, obs_value								as [value_as_string]
	, NULL									as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase II %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code=  pat.obs_name
	where pat.obs_name like 'S2%'
	
	union

	--phase III 
	select pat.[IDNO]						as person_id
	, pat.obs_date							as observation_date
	, obs_name + ' = '
		+ obs_value							as observation_source_value
	, obs_value								as [value_as_string]
	, NULL									as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase III %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code=  pat.obs_name
	where pat.obs_name like 'S3%'

)a

