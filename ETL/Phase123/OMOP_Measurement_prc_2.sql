/********************************************************************
Name: ETL to populate OMOP Measurement Table

Source Data tables: SHS Phase123
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

	
use omoprawdata 
go


---- Insert SHS Phase I, II, III vitals data (%Body fat -RJL)
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
, 8554							as unit_concept_id    --%
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
	--phase I %Body fat -RJL 
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1BDFAT'								as measurement_source_value
	, S1BDFAT									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= 'S1BDFAT'
	where pat.S1BDFAT is not null 

	union

	--phase II %Body fat -RJL
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2BDFAT'								as measurement_source_value
	, S2BDFAT									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join omop1.dbo.source_to_concept_map shs2 on shs2.source_code= 'S2BDFAT'
	where pat.S2BDFAT is not null 
	
	union

	--phase III %Body fat -RJL
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3BDFAT'								as measurement_source_value
	, S3BDFAT									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join omop1.dbo.source_to_concept_map shs3 on shs3.source_code= 'S3BDFAT'
	where pat.S3BDFAT is not null 

)a
---------------------------
 
---- Insert SHS Phase I, II, III vitals data (Pulse pressure  )
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
, 8876							as unit_concept_id    --	millimeter mercury column
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
	--phase I Pulse pressure
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1PP'								as measurement_source_value
	, S1PP									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= 'S1PP'
	where pat.S1PP is not null 

	union

	--phase II Pulse pressure
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2PP'								as measurement_source_value
	, S2PP									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join omop1.dbo.source_to_concept_map shs2 on shs2.source_code= 'S2PP'
	where pat.S2PP is not null 
	
	union

	--phase III Pulse pressure
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3PP'								as measurement_source_value
	, S3PP									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join omop1.dbo.source_to_concept_map shs3 on shs3.source_code= 'S3PP'
	where pat.S3PP is not null 

)a
---------------------------


---- Insert SHS Phase I, II, III vitals data (mean arterial pressure  )
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
, 8876							as unit_concept_id    --	millimeter mercury column
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
	--phase I mean arterial  pressure
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1MBP'								as measurement_source_value
	, S1MBP									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= 'S1MBP'
	where pat.S1MBP is not null 

	union

	--phase II mean arterial  pressure
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2MBP'								as measurement_source_value
	, S2MBP									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join omop1.dbo.source_to_concept_map shs2 on shs2.source_code= 'S2MBP'
	where pat.S2MBP is not null 
	
	union

	--phase III mean arterial  pressure
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3MBP'								as measurement_source_value
	, S3MBP									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join omop1.dbo.source_to_concept_map shs3 on shs3.source_code= 'S3MBP'
	where pat.S3MBP is not null 

)a
---------------------------