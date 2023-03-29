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

-- Unpivot the source table. (unit mL/min ) 
if object_id('tempdb.dbo.#mL_min') is not null drop table #mL_min 
SELECT IDNO, Lab_name, Lab_value, 
	cast(NULL as datetime) as Lab_date,  
	cast(NULL as varchar(10)) as Phase
into #mL_min
FROM   
   (SELECT [IDNO]
	, S1GFR_EPI
	
	,S2GFR_EPI

	,S3GFR_EPI

   FROM omoprawdata.dbo.SHSALL33 ) p  
UNPIVOT  
   (Lab_value FOR Lab_name IN   

     ( S1GFR_EPI	,S2GFR_EPI	,S3GFR_EPI )  
)AS unpvt;  
GO


--add the measurement date
update mg set Lab_date = case 
	when lab_name like '_1%' then rw.S1EXDATE 
	when lab_name like '_2%' then rw.S2EXDATE 
	when lab_name like '_3%' then rw.S3EXDATE 
end
, Phase = case 
	 when lab_name like '_1%' then 'Phase I'
	when lab_name like '_2%' then 'Phase II'
	when lab_name like '_3%' then 'Phase III'
end
 from #mL_min mg
join omoprawdata.dbo.SHSALL33 rw on rw.idno = mg.idno 

select * from #mL_min
--[omoprawdata].dbo.[SHSALL33]




---- Insert SHS Phase I, II, III  labs with unit mL/min
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
, 0								as value_as_concept_id
, 8795							as unit_concept_id    --mL/min
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, 'mL/min'						as unit_source_value
, a.measurement_value			as value_source_value

from (
	select pat.[IDNO]						as person_id
	, pat.Lab_date							as measurement_date
	, Lab_name								as measurement_source_value
	, Lab_value								as measurement_value
	, case when pat.phase = 'Phase I' then vo1.visit_occurrence_id				
			when pat.phase = 'Phase II' then vo2.visit_occurrence_id
			when pat.phase = 'Phase III' then vo3.visit_occurrence_id
	 end									as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	from #mL_min pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase I %'
	left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] 
		and vo2.visit_source_value like 'SHS Phase II %'
	left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] 
		and vo3.visit_source_value like 'SHS Phase III %'

	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
)a