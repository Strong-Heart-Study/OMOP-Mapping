use omop1


/***************************************************************************************************

-- Create date: 10/04/2021
-- Description: ETL to map WT, HT and BMI information from SHSALL (phase6) to OMOP Measurement table

*****************************************************************************************************/




use omop1
drop table if exists #meas 
select idno, vble, value
, S6EXDATE meas_date 
into #meas 
from (
	select idno, S6EXDATE
	, S6WT, S6HT, S6BMI
	from omoprawdata.dbo.s6all
) p
unpivot
( value for vble in (
	 S6WT, S6HT, S6BMI
	)
) as unpvt;


-----------------------------------------------
/*
source_code		concept_id	concept_code	concept_name						domain			vocab		
---------		-----------	------------	-------------						--------		-------		
S6WT			3025315		29463-7			Body weight							Measurement		LOINC		
S6HT			3036277		8302-2			Body height							Measurement		LOINC		
S6BMI			3038553		39156-5			Body mass index (BMI) [Ratio]		Measurement		LOINC		

*/

--update source to concept map
insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
	, valid_start_date, valid_end_date)
select 'S6WT', 0, 'SHS', 3025315, 'LOINC', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
select 'S6HT', 0, 'SHS', 3036277, 'LOINC', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
select 'S6BMI', 0, 'SHS', 3038553, 'LOINC', '1996-09-08',	'2099-12-31' 

-----------------------------------------------


---- Insert SHS Phase 6 Labs data 
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
, a.measurement_value			as value_as_number
, NULL							as value_as_concept_id
, a.unit_concept_id				as unit_concept_id    
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, a.unit_source_value			as unit_source_value
, a.measurement_source_value	as value_source_value

from (
	select pat.[IDNO]										as person_id
	, pat.meas_date											as measurement_date
	, pat.vble + cast(pat.[value] as varchar(20))			as measurement_source_value
	, pat.[value]											as measurement_value
	, vo1.visit_occurrence_id								as visit_occurrence_id
	, shs1.target_concept_id								as target_concept_id 
	, case when vble = 'S6WT' then 9529									-- Kilograms
			when vble = 'S6HT' then 8582								--centimeter
			when vble = 'S6BMI' then 0									-- no unit
			end												as unit_concept_id
	, case when vble = 'S6WT' then 'Kilograms'
			when vble = 'S6HT' then 'centimeter'
			when vble = 'S6BMI' then NULL 
			end												as unit_source_value
	from #meas pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase VI %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble
	
)a

