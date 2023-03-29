DROP TABLE IF EXISTS #allvaluestemp
SELECT idno ,
       Diagnosis_Age,
       Phase,
       Visit_date ,
	   observation_source_value
	   
INTO #allvaluestemp
FROM
  (SELECT idno,
          s1exdate AS Visit_date,
          MED27 AS Diagnosis_Age,
          1 AS Phase,
          concat('s1dmage (MED27) ' , MED27) as observation_source_value
   FROM omoprawdata.dbo.SHSALL33
   UNION SELECT idno,
                s2exdate AS Visit_date,
                MED2_7 AS Diagnosis_Age,
                2 AS Phase,
                concat('s2dmage (MED2_7) = ' , MED2_7) as observation_source_value
   FROM omoprawdata.dbo.SHSALL33
   UNION SELECT idno,
                s3exdate AS Visit_date,
                MED3_10 AS Diagnosis_Age,
                3 AS Phase,
                concat('s3dmage (MED3_10) = ' , MED3_10) as observation_source_value
   FROM omoprawdata.dbo.SHSALL33
   UNION SELECT idno,
                s4exdate AS Visit_date,
                MED4_10 AS Diagnosis_Age,
                4 AS Phase,
                concat('s4dmage (MED4_10) = ' , MED4_10) as observation_source_value
   FROM omoprawdata.dbo.S4ALL23
   UNION SELECT idno ,
                s5exdate AS Visit_date,
                floor(S5AGE)-S5ADADMD AS Diagnosis_Age,
                5 AS Phase,
                concat('s5dmage (floor(S5AGE)-S5ADADMD) = ' , floor(S5AGE)-S5ADADMD) as observation_source_value
   FROM omoprawdata.dbo.S5ALL23
   UNION SELECT idno,
                s6exdate AS Visit_date,
                s6dmage AS Diagnosis_Age,
                6 AS Phase,
                concat('s6dmage = ' , s6dmage) as observation_source_value
   FROM omoprawdata.dbo.S6ALL
   WHERE s6dmage != '') AS a
WHERE a.Diagnosis_Age IS NOT NULL


SELECT * from #allvaluestemp ORDER BY IDNO, PHASE;

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
select
next Value for  omop1.dbo.observation_seq as [observation_id]
      , a.[idno]                    as person_id
      ,    4307859                     as [observation_concept_id]  -- age at diagnosis
      ,    a.Visit_date                as [observation_date]
      , a.Visit_date                as [observation_datetime]
      , 44814721                        as [observation_type_concept_id]  --    Patient reported
      , a.Diagnosis_Age                as [value_as_number]
      , 201820                            as [qualifier_concept_id]
      , 9448                            as [unit_concept_id] --    year
      , 1                                as [provider_id]
      , a.visit_occurrence_id            as [visit_occurrence_id]
      , a.observation_source_value        as [observation_source_value]
      , 0                                as [observation_source_concept_id]
      , 'year'                            as [unit_source_value]
      , 'Diabetes mellitus'                            as [qualifier_source_value]
      , 0                                as [obs_event_field_concept_id]
from (
    select * from #allvaluestemp
    left join dbo.visit_occurrence vo on vo.person_id = #allvaluestemp.[IDNO] 
    and  vo.visit_start_datetime = #allvaluestemp.Visit_date
)as a

select * from omop1.dbo.source_to_concept_map
----S1---
Insert into omop1.dbo.source_to_concept_map
(source_code, source_concept_id,source_vocabulary_id,source_code_description
,target_concept_id,target_vocabulary_id,valid_start_date,valid_end_date,invalid_reason,target_qualifier_id)
values
(	's1dmage (MED27)',0,'SHS','S1 Diabetes diagnosis age',
	4307859,'SNOMED','1996-09-08','2099-12-31',NULL,201820
)


---S2---
Insert into omop1.dbo.source_to_concept_map
(source_code, source_concept_id,source_vocabulary_id,source_code_description
,target_concept_id,target_vocabulary_id,valid_start_date,valid_end_date,invalid_reason,target_qualifier_id)
values
(	's2dmage (MED2_7)',0,'SHS','S2 Diabetes diagnosis age',
	4307859,'SNOMED','1996-09-08','2099-12-31',NULL,201820
)


----S3---
Insert into omop1.dbo.source_to_concept_map
(source_code, source_concept_id,source_vocabulary_id,source_code_description
,target_concept_id,target_vocabulary_id,valid_start_date,valid_end_date,invalid_reason,target_qualifier_id)
values
(	's3dmage (MED3_10)',0,'SHS','S3 Diabetes diagnosis age',
	4307859,'SNOMED','1996-09-08','2099-12-31',NULL,201820
)

---S4---
Insert into omop1.dbo.source_to_concept_map
(source_code, source_concept_id,source_vocabulary_id,source_code_description
,target_concept_id,target_vocabulary_id,valid_start_date,valid_end_date,invalid_reason,target_qualifier_id)
values
(	's4dmage (MED4_10) ',0,'SHS','S4 Diabetes diagnosis age',
	4307859,'SNOMED','1996-09-08','2099-12-31',NULL,201820
)

---S5---
Insert into omop1.dbo.source_to_concept_map
(source_code, source_concept_id,source_vocabulary_id,source_code_description
,target_concept_id,target_vocabulary_id,valid_start_date,valid_end_date,invalid_reason,target_qualifier_id)
values
(	's5dmage (floor(S5AGE)-S5ADADMD)',0,'SHS','S5 Diabetes diagnosis age',
	4307859,'SNOMED','1996-09-08','2099-12-31',NULL,201820
)
----S6-----
Insert into omop1.dbo.source_to_concept_map
(source_code, source_concept_id,source_vocabulary_id,source_code_description
,target_concept_id,target_vocabulary_id,valid_start_date,valid_end_date,invalid_reason,target_qualifier_id)
values
(	's6dmage',0,'SHS','S6 Diabetes diagnosis age',
	4307859,'SNOMED','1996-09-08','2099-12-31',NULL,201820
)




