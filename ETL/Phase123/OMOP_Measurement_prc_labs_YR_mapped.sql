 
/********************************************************************
Name: ETL to populate OMOP Measurement Table
 
Source Data tables: SHS Phase123 SHSALL33
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE
Destination table: Measurement

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'omoprawdata.dbo.S4ALL23' 
		to the appropriate table that contains SHS Phase4 data

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


-- Unpivot the source table. 
if object_id('tempdb.dbo.#labs') is not null drop table #labs
SELECT IDNO, Lab_name, Lab_value, 
	cast(NULL as datetime) as Lab_date,  
	cast('' as varchar(50)) as Unit,
	cast(NULL as varchar(10)) as Phase
into #labs
FROM   
   (SELECT [IDNO]
	, S1LACR
	,S1FIBRIN
	,S1APOB
	,S1APOAI
	,S2LACR
	,S2FIBRIN
	,S3FIBRIN
	,S3LACR
	,S1CCR
	,S1HOMAIR
	,S2CCR
	,S2HOMAIR
	,S3CCR
	,S3HOMAIR
	,S2PAI1
   FROM [omoprawdata].dbo.SHSALL33 ) p  
UNPIVOT  
   (Lab_value FOR Lab_name IN   
      (	 S1LACR 
		,S1FIBRIN 
		,S1APOB 
		,S1APOAI 
		,S2LACR 
		,S2FIBRIN 
		,S3FIBRIN 
		,S3LACR 
		,S1CCR 
		,S1HOMAIR 
		,S2CCR 
		,S2HOMAIR 
		,S3CCR 
		,S3HOMAIR 
		,S2PAI1
	  )  
)AS unpvt;  
GO


select * from #labs

--add the measurement date and units 
update mg set Lab_date = case 
		when Lab_name like '_1%' then  rw.S1EXDATE 
		when Lab_name like '_2%' then  rw.S2EXDATE
		when Lab_name like '_3%' then  rw.S3EXDATE 
	end
, Phase = case 
		when Lab_name like '_1%' then  'Phase I' 
		when Lab_name like '_2%' then  'Phase II' 
		when Lab_name like '_3%' then  'Phase III' 
	end

	--clean here
, Unit = case 
	when Lab_name IN ( 'S1LACR', 'S2LACR', 'S3LACR') then 'mg/g' 
	when Lab_name IN ( 'S1FIBRIN', 'S2FIBRIN','S3FIBRIN', 'S1APOB', 'S1APOAI') then 'mg/dL' 
	when Lab_name IN ( 'S1CCR', 'S2CCR','S3CCR') then 'mm/min'
	when Lab_name in ('S1HOMAIR','S2HOMAIR','S3HOMAIR','S2PAI1') then NULL
	else NULL 
	end 
 from #labs mg
join [omoprawdata].dbo.SHSALL33 rw on rw.idno = mg.idno 






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
, a.measurement_value			as  value_as_number
, 0								as value_as_concept_id
, a.OMOP_Unit					as unit_concept_id    
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, a.raw_unit					as unit_source_value
, a.measurement_value			as value_source_value

from (
	select pat.[IDNO]						as person_id
	, pat.Lab_date							as measurement_date
	, Lab_name								as measurement_source_value
	, Lab_value								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, pat.Unit								as raw_unit
	, case 
		when pat.Unit = 'mg/dL' then 8840 --mg/dL
		when pat.Unit = 'mg/g' then 8723	---milligram per gram
		when pat.Unit = 'mm/min' then 8795 -- per google search it is mL/min
		when pat.Unit is null then 0 
		end									as OMOP_Unit
	from #labs pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
	where pat.Lab_name like 'S1%'

UNION

	select pat.[IDNO]						as person_id
	, pat.Lab_date							as measurement_date
	, Lab_name								as measurement_source_value
	, Lab_value								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, pat.Unit								as raw_unit
	, case 
		when pat.Unit = 'mg/dL' then 8840 --mg/dL
		when pat.Unit = 'mg/g' then 8723	---milligram per gram
		when pat.Unit = 'mm/min' then 0 -- (check if typo)
		when pat.Unit is null then 0 
		end									as OMOP_Unit
	from #labs pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase II %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
	where pat.Lab_name like 'S2%'

UNION

	select pat.[IDNO]						as person_id
	, pat.Lab_date							as measurement_date
	, Lab_name								as measurement_source_value
	, Lab_value								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, pat.Unit								as raw_unit
	, case 
		when pat.Unit = 'mg/dL' then 8840 --mg/dL
		when pat.Unit = 'mg/g' then 8723	---milligram per gram
		when pat.Unit = 'mm/min' then 8795	---milliliter per minute (per google search, unit for COCKROFT-GAULT CREATININE CLEARANCE is mL/min)
		when pat.Unit is null then 0 
		end									as OMOP_Unit
	from #labs pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase III %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
	where pat.Lab_name like 'S3%'
		
)a

---------------------------


