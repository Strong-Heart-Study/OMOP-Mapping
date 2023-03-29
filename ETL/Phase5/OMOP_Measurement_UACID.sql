/********************************************************************
Name: ETL to populate OMOP Measurement Table

Source Data tables: SHS Phase4
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE
Destination table: Measurement

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'omoprawdata.dbo.SHSALL33' 
		to the appropriate table that contains SHS Phase123 data

	3) Change omop1.dbo.source_to_concept_map to the 
		appropriate db/table name.

	4) Identify units in the source data for Labs. Change the unit_source_value and 
		unit_concept_id accordingly. Use SQL below to identify correct unit

		==============================================================================
			Select * from omop_vocabulary.vocab51.concept where domain_id = 'Unit'
			and standard_concept = 'S'
		==============================================================================

	5) range_low and range_high are currently set to 0. Please update these 
		values if they're available in source data.

*********************************************************************/

use omop1 
go


if object_id('tempdb.dbo.#uacid_temp') is not null drop table #uacid_temp 
SELECT IDNO, S5UACID, 
	cast(NULL as datetime) as Lab_date
into #uacid_temp
FROM omoprawdata.dbo.S5ALL23;  
GO


--add the measurement date
update mg set Lab_date = rw.S5EXDATE
 from #uacid_temp mg
join [omoprawdata].dbo.S5ALL23 rw on rw.idno = mg.idno 

select * from #uacid_temp



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
select  next Value for  omop1.dbo.measurement_seq as measurement_id
, a.person_id					as person_id
, 3037556						as measurement_concept_id
, a.measurement_date			as measurement_date
, a.measurement_date			as measurement_datetime
, NULL							as measurement_time
, 44818702						as measurement_type_concept_id  --(lab result) ---select * from OMOP_VOCABULARY.vocab51.CONCEPT where vocabulary_id = 'Meas Type' 
, 4172703						as operator_concept_id
, a.measurement_value			as  value_as_number
, 0								as value_as_concept_id
, 8840							as unit_concept_id    --	mg/dL
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, 'mg/dL'						as unit_source_value
, a.measurement_value			as value_source_value
from (
	select pat.[IDNO]						as person_id
	, pat.Lab_date							as measurement_date
	, 'S5UACID'								as measurement_source_value
	, S5UACID								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	from #uacid_temp pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
)a




