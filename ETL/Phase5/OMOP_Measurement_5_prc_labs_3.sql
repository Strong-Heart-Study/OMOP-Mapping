 
/********************************************************************
Name: ETL to populate OMOP Measurement Table
 
Source Data tables: SHS Phase4 S4ALL23
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
	, S5CRP
	, S5HGB
	, S5INSU
	, S5PLT
	, S5U_ALB
	, S5FAT
	, S5UCREA
   FROM [omoprawdata].dbo.S5ALL23 ) p  
UNPIVOT  
   (Lab_value FOR Lab_name IN   
      (	 S5CRP
		, S5HGB
		, S5INSU
		, S5PLT
		, S5U_ALB	  
		, S5FAT
		, S5UCREA
	  )  
)AS unpvt;  
GO


--add the measurement date and units 
update mg set Lab_date = rw.s5exdate
, Phase = 'Phase V'
, Unit = case 
	when Lab_name = 'S5AST' then 'iU/L' 
	when Lab_name = 'S5CRP' then 'mg/L' 
	when Lab_name = 'S5HGB' then 'g/dL' 
	when Lab_name = 'S5INSU' then 'uU/mL' 
	when Lab_name = 'S5PLT' then '10**9/L' 

	when Lab_name = 'S5POT' then 'mEq/L' 
	when Lab_name = 'S5TPROT' then 'gm/dL' 
	when Lab_name = 'S5U_ALB' then 'mg/dL' 

	when Lab_name = 'S5FAT' then 'kg' 
	when Lab_name = 'S5UCREA' then 'mg/dL' 

	else NULL 
	end 
 from #labs mg
join [omoprawdata].dbo.S5ALL23 rw on rw.idno = mg.idno 



select * from #labs


---- Insert SHS Phase IV Labs data (with unit IU/L  )
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
	, case when pat.unit = 'iU/L' then 8923 --international unit per liter	Unit		[iU]/L
		when pat.unit = 'mg/L' then 8751
		when pat.unit = 'g/dL' then 8713
		when pat.unit = 'uU/mL' then 8860
		when pat.unit = '10**9/L' then 9444
		when pat.Unit = 'mEq/L' then 9557
		when pat.Unit = 'gm/dL' then 8713 -- gram per deciliter
		when pat.Unit = 'mg/dL' then 8840
		when pat.Unit = 'kg' then 9529 -- Kilogram
		when pat.Unit = 'cm' then 8582 -- cm
		end									as OMOP_Unit
	from #labs pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
)a

---------------------------


