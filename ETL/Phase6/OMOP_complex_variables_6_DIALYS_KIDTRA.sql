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
	convert(datetime, S6EXDATE, 121) as obs_date,  
	'Phase VI' as Phase,
	case 
		when obs_value = '1' then 4188539 --Yes
		when obs_value = '2' then 4188540 --No
		when obs_value = '3' then 4129922 --Unknown
	end as value_concept_id
into #obs
FROM   
   (SELECT [IDNO], S6EXDATE
	, S6DIALYS
	, S6KIDTRA
   FROM [omoprawdata].dbo.s6all ) p  
UNPIVOT  
   (obs_value FOR obs_name IN   
      (	 
	 S6DIALYS
	, S6KIDTRA	
	  )  
)AS unpvt;  
GO


-- check values 
select obs_name, obs_value, count(*) from #obs group by obs_name, obs_value order by obs_name, obs_value



/*
--add the Observation date and units 
update mg 
set value_concept_id = case 
		when obs_value = '1' then 4188539 --Yes
		when obs_value = '2' then 4188540 --No
		when obs_value = '3' then 4129922 --Unknown
	end
 from #obs mg
 */


select * from #obs

---------------------------------

begin 
	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_code_description, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--4059475	H/O: renal dialysis	Observation	SNOMED	Context-dependent	S	161693006	2002-01-31	2099-12-31	NULL
	select 'S6DIALYS', 0, 'SHS6 RENAL DIALYSIS', 'SHS', 4059475, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	
	--4059356	History of renal transplant	Observation	SNOMED	Context-dependent	S	161665007	2002-01-31	2099-12-31	NULL
	select 'S6KIDTRA', 0, 'SHS6 KIDNEY TRANSPLANT', 'SHS', 4059356, 'SNOMED', '1996-09-08',	'2099-12-31' -- condition mapping (standard concept)


END 

----------------------------------

-- Phase 6
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
	, value_concept_id						as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase VI'								as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase VI %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.obs_name
	where pat.obs_name like 'S6%'

	

)a
