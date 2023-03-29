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
select next Value for  omoprawdata.dbo.observation_seq as [observation_id]
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
	--Phase V  lean body mass (kg)
	select pat.[IDNO]						as person_id
	, pat.S5EXDATE							as observation_date
	, 'S5LBM = ' 
		+ cast(S5LBM as varchar(10))		as observation_source_value
	, S5LBM									as value_as_number
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase V'							as phase
	from omoprawdata.dbo.S5ALL23 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S5LBM'
	where pat.S5LBM is not null 

)a
