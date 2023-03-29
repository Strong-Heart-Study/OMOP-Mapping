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
select next Value for  omop1.dbo.observation_seq  as [observation_id]
      , a.[person_id]                       as person_id
      , 4269436                             as [observation_concept_id]
      , a.observation_date                  as [observation_date]
      , a.observation_date                  as [observation_datetime]
      , 44814721                            as [observation_type_concept_id]  --            Patient reported
      , a.value_as_number                   as value_as_number
      , a.[value_as_concept_id]             as [value_as_concept_id]
      , null                                as [unit_concept_id] 
      , 1                                   as [provider_id]
      , a.visit_occurrence_id               as [visit_occurrence_id]
      , a.observation_source_value          as [observation_source_value]
      , 0                                   as [observation_source_concept_id]
      , null                                as [unit_source_value]
      , 4145082                             as [qualifier_source_value] -- Frequency per year
      , 0                                   as [obs_event_field_concept_id]

from (
      select pat.[IDNO]            as person_id
      , pat.S5EXDATE               as observation_date
      ,case 
            when pat.S5BINGEY = 0 then 'S5BINGEY = 0 No'
            when pat.S5BINGEY = 1 then 'S5BINGEY = 1 binge drinker'
            when pat.S5BINGEY = 2 then 'S5BINGEY = 2 heavy binge drinker'
      end                          as observation_source_value
      , pat.S5BINGEY               as value_as_number
       ,case 
            when pat.S5BINGEY = 0 then 4188540 -- no
            when pat.S5BINGEY = 1 then 4027367 -- binge
            when pat.S5BINGEY = 2 then 4336673 -- heavy drinker
      end 
                                   as [value_as_concept_id]
      , vo1.visit_occurrence_id    as visit_occurrence_id 
      from omoprawdata.dbo.S5ALL23 pat
      left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
            and vo1.visit_source_value like 'SHS Phase V %'
                  where S5BINGEY is not null


)a
