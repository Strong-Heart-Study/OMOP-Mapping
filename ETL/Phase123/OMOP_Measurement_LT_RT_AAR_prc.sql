
/***************************************************************************************************

-- Author: Paulina Paul
-- Create date: 10/04/2021
-- Description: ETL to map LT_AAR and RT_AAR information from SHSALL (phase123) to OMOP Measurement table

*****************************************************************************************************/

/*
source_code		concept_id	concept_code	concept_name			domain			vocab		value_as_concept_id		value_as_concept_name
---------		-----------	------------	-------------			--------		-------		--------------------	------------------------
S1RT_AAR		46237026		77194-9		Ankle-brachial index	Measurement		LOINC		45881626				Right
S1LT_AAR		46237026		77194-9		Ankle-brachial index	Measurement		LOINC		45883143				Left

S2RT_AAR		46237026		77194-9		Ankle-brachial index	Measurement		LOINC		45881626				Right
S2LT_AAR		46237026		77194-9		Ankle-brachial index	Measurement		LOINC		45883143				Left

S3RT_AAR		46237026		77194-9		Ankle-brachial index	Measurement		LOINC		45881626				Right
S3LT_AAR		46237026		77194-9		Ankle-brachial index	Measurement		LOINC		45883143				Left

*/


use omop1
drop table if exists #aar 
select idno, s1exdate, s2exdate, s3exdate, vble, value
, cast(null as datetime) meas_date 
, cast(0 as bigint) value_as_concept_id
, cast('' as varchar(20)) value_source_value 
into #aar
from (
	select idno, S1EXDATE, S2EXDATE, S3EXDATE
	, S1RT_AAR, S1LT_AAR
	, S2RT_AAR, S2LT_AAR
	, S3RT_AAR, S3LT_AAR
	from omoprawdata.dbo.SHSALL33
) p
unpivot
( value for vble in (
	 S1RT_AAR, S1LT_AAR
	, S2RT_AAR, S2LT_AAR
	, S3RT_AAR, S3LT_AAR
	)
) as unpvt;


update #aar
set meas_date = case when vble like 's1%' then S1EXDATE
					when vble like 's2%' then S2EXDATE
					when vble like 's3%' then S3EXDATE
				end
	, value_as_concept_id = case when vble like '%lt%' then 45883143
								  when vble like '%rt%' then 45881626
							end 
	, value_source_value = case when vble like '%lt%' then 'Left'
									when vble like '%rt%' then 'Right'
								end 

select * from #aar


-----------------------------------------------

--update source to concept map
insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
	, valid_start_date, valid_end_date)

--S4RT_AAR		46237026		77194-9		Ankle-brachial index
select 'S1RT_AAR', 0, 'SHS', 46237026, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
select 'S2RT_AAR', 0, 'SHS', 46237026, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
select 'S3RT_AAR', 0, 'SHS', 46237026, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)

select 'S1LT_AAR', 0, 'SHS', 46237026, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
select 'S2LT_AAR', 0, 'SHS', 46237026, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
select 'S3LT_AAR', 0, 'SHS', 46237026, 'SNOMED', '1996-09-08',	'2099-12-31'  -- condition mapping (standard concept)


-----------------------------------------------


---- Insert SHS Phase 123 Labs data 
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
, value_as_concept_id			as value_as_concept_id
, NULL							as unit_concept_id    
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, NULL							as unit_source_value
, a.value_source_value			as value_source_value

from (
	select distinct pat.[IDNO]						as person_id
	, pat.meas_date							as measurement_date
	, pat.[value]							as measurement_source_value
	, pat.[value]							as measurement_value
	, pat.value_as_concept_id				as value_as_concept_id
	, pat.value_source_value				as value_source_value
	, case when vble like 's1%' then vo1.visit_occurrence_id			
			when vble like 's2%' then vo2.visit_occurrence_id
			when vble like 's3%' then vo3.visit_occurrence_id
			end								as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	from #aar pat 
	inner join omop1.dbo.person p on p.person_id=pat.IDNO
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'	
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble
	
)a
