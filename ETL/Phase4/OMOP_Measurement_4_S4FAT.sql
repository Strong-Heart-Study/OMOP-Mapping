
use omoprawdata 
go

-- Add mapping to source_to_concept_map table
begin 
	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_code_description, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--4087498	Total body fat	Measurement	SNOMED	Observable Entity	S	248361005	2002-01-31	2099-12-31	NULL
	select 'S4FAT', 0, 'SHS4 BODY FAT (kg)', 'SHS', 4087498, 'SNOMED', '1996-09-08',	'2099-12-31' -- condition mapping (standard concept)
	

END 

----------------------------------------------------------------

---- Insert SHS Phase IV vitals data (Body fat)
insert into omop1.dbo.measurement (
	 [measurement_id]
      ,[person_id]
      ,[measurement_concept_id]
      ,[measurement_date]
      ,[measurement_datetime]
	  ,[measurement_time]
      ,[measurement_type_concept_id]
      ,[operator_concept_id]
      ,[value_as_number]
      ,[value_as_concept_id]
      ,[unit_concept_id]
      ,[range_low]
      ,[range_high]
      ,[provider_id]
      ,[visit_occurrence_id]
	  ,[visit_detail_id]
      ,[measurement_source_value]
      ,[measurement_source_concept_id]
      ,[unit_source_value]
      ,[value_source_value]
	  )
select  next Value for  dbo.measurement_seq as measurement_id
, a.person_id					as person_id
, a.target_concept_id			as measurement_concept_id
, a.measurement_date			as measurement_date
, a.measurement_date			as measurement_datetime
, NULL							as measurement_time
, 44818702						as measurement_type_concept_id  --(lab result) ---select * from OMOP_VOCABULARY.vocab51.CONCEPT where vocabulary_id = 'Meas Type' 
, 4172703						as operator_concept_id
, a.measurement_value			as  value_as_number
, 0								as value_as_concept_id
, 9529							as unit_concept_id    --kg
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, NULL							as unit_source_value
, a.measurement_value			as value_source_value

from (
	--phase IV Body fat  
	select pat.[IDNO]						as person_id
	, convert(datetime, pat.S4EXDATE, 121)	as measurement_date
	, 'S4FAT'								as measurement_source_value
	, S4FAT									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase IV'							as phase
	from dbo.S4ALL23 pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase IV %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= 'S4FAT'
	where pat.S4FAT is not null 

	
)a
---------------------------
