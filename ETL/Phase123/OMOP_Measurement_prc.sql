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


*********************************************************************/

	
use strongHeart_OMOP 
go


--create Measurement sequence if it does not exist
IF EXISTS (SELECT name FROM sys.sequences WHERE name = N'measurement_seq')
	ALTER SEQUENCE dbo.measurement_seq RESTART WITH 1 INCREMENT BY 1  
ELSE 
	CREATE SEQUENCE dbo.measurement_seq  START WITH 1 INCREMENT BY 1;	
;  
GO  





----====================================================================

---- Insert SHS Phase I, II, III vitals data (Sys BP)
insert into dbo.measurement (
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
, 8876							as unit_concept_id    --millimeter mercury column
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
	--phase I Systolic Blood pressure 
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1SBP'								as measurement_source_value
	, S1SBP									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1SBP'
	where pat.S1SBP is not null 

	union

	--phase II Systolic Blood pressure 
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2SBP'								as measurement_source_value
	, S2SBP									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2SBP'
	where pat.S2SBP is not null 
	
	union

	--phase III Systolic Blood pressure 
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3SBP'								as measurement_source_value
	, S3SBP									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3SBP'
	where pat.S3SBP is not null 

)a



----====================================================================


---- Insert SHS Phase I, II, III vitals data (dias BP)
insert into dbo.measurement (
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
, 8876							as unit_concept_id    --millimeter mercury column
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
	--phase I diastolic Blood pressure 
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1DBP'								as measurement_source_value
	, S1DBP									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1DBP' 
	where pat.S1DBP is not null 

	union

	--phase II diastolic Blood pressure 
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2DBP'								as measurement_source_value
	, S2DBP									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2DBP'
	where pat.S2DBP is not null 
	
	union

	--phase III diastolic Blood pressure 
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3DBP'								as measurement_source_value
	, S3DBP									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3DBP'
	where pat.S3DBP is not null 

)a


----====================================================================


---- Insert SHS Phase I, II, III vitals data (weight S1WT)
insert into dbo.measurement (
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
, 9529							as unit_concept_id  --Kilograms  
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
	--phase I weight
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1WT'								as measurement_source_value
	, S1WT									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase	
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1WT'
	where pat.S1WT is not null 

	union

	--phase II weight
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2WT'								as measurement_source_value
	, S2WT									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase	
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2WT'
	where pat.S2WT is not null 
	
	union

	--phase III weight
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3WT'								as measurement_source_value
	, S3WT									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase	
	from dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3WT'
	where pat.S3WT is not null 

)a



----====================================================================


---- Insert SHS Phase I, II, III vitals data (height)
insert into dbo.measurement (
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
, 8582							as unit_concept_id    --centimeter
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
	--phase I height
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1HT'								as measurement_source_value
	, S1HT									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from omoprawdata.dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1HT'
	where pat.S1HT is not null 

	union

	--phase II height
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2HT'								as measurement_source_value
	, S2HT									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from omoprawdata.dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2HT'
	where pat.S2HT is not null 
	
	union

	--phase III height
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3HT'								as measurement_source_value
	, S3HT									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from omoprawdata.dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3HT'
	where pat.S3HT is not null 


)a



----====================================================================


---- Insert SHS Phase I, II, III vitals data (BMI)
insert into dbo.measurement (
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
, 0								as value_as_concept_id
, 0								as unit_concept_id    --no unit
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
	--phase I BMI
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1BMI'								as measurement_source_value
	, S1BMI									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from omoprawdata.dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1BMI'
	where pat.S1BMI is not null 

	union

	--phase II BMI
	select pat.[IDNO]						as person_id
	, pat.S2EXDATE							as measurement_date
	, 'S2BMI'								as measurement_source_value
	, S2BMI									as measurement_value
	, vo2.visit_occurrence_id				as visit_occurrence_id
	, shs2.target_concept_id				as target_concept_id 
	, 'Phase II'							as phase
	from omoprawdata.dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join dbo.source_to_concept_map shs2 on shs2.source_code= 'S2BMI'
	where pat.S2BMI is not null 
	
	union

	--phase III BMI
	select pat.[IDNO]						as person_id
	, pat.S3EXDATE							as measurement_date
	, 'S3BMI'								as measurement_source_value
	, S3BMI									as measurement_value
	, vo3.visit_occurrence_id				as visit_occurrence_id
	, shs3.target_concept_id				as target_concept_id 
	, 'Phase III'							as phase
	from omoprawdata.dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'
	left join dbo.source_to_concept_map shs3 on shs3.source_code= 'S3BMI'
	where pat.S3BMI is not null 

)a


----====================================================================


---- Insert SHS Phase I, II, III vitals data (Body surface area)
insert into dbo.measurement (
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
, 8617							as unit_concept_id    --square meter (m^2)
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
	--phase I diastolic Blood pressure 
	select pat.[IDNO]						as person_id
	, pat.S1EXDATE							as measurement_date
	, 'S1BSA'								as measurement_source_value
	, S1BSA									as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase I'								as phase
	from omoprawdata.dbo.SHSALL33 pat 
	left join dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join dbo.source_to_concept_map shs1 on shs1.source_code= 'S1BSA'
	where pat.S1BSA is not null 

)a


----================