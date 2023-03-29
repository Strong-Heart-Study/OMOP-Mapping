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



-- Unpivot the source table. (unit mg/dL ) 
if object_id('tempdb.dbo.#mg_dL') is not null drop table #mg_dL 
SELECT IDNO, Lab_name, Lab_value, 
	cast(NULL as datetime) as Lab_date,  
	cast(NULL as varchar(10)) as Phase
into #mg_dL
FROM   
   (SELECT [IDNO]
   , S5TG
	,S5TC
	,S5HDL
	,S5G0
	,S5P_CREA

	--,S5FIBRIN
	,S5LDL
	--,S5BUN
	--,S5CAL
	--,S5PHOSP
	--,S5TBILI
	--,S5APOA1
	--,S5APOB
	--,S5U_CREA


   FROM[omoprawdata].dbo.S5ALL23 ) p  
UNPIVOT  
   (Lab_value FOR Lab_name IN   
     (
		S5TG
		,S5TC
		,S5HDL
		,S5G0
		,S5P_CREA
	
		--,S5FIBRIN
		,S5LDL
		--,S5BUN
		--,S5CAL
		--,S5PHOSP
		--,S5TBILI
		--,S5APOA1
		--,S5APOB
		--,S5U_CREA

	  )  
)AS unpvt;  
GO


--add the measurement date
update mg set Lab_date = rw.S5EXDATE
 from #mg_dL mg
join [omoprawdata].dbo.S5ALL23 rw on rw.idno = mg.idno 

select * from #mg_dL

select distinct LAb_name from #mg_dL


---- Insert SHS Phase I, II, III Lab data ( mg/dL)
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
	, Lab_name								as measurement_source_value
	, Lab_value								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	from #mg_dL pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
)a



---------------------------

-- Unpivot the source table. (unit mL/min ) 
if object_id('tempdb.dbo.#mL_min') is not null drop table #mL_min 
SELECT IDNO, Lab_name, Lab_value, 
	cast(NULL as datetime) as Lab_date,  
	cast(NULL as varchar(10)) as Phase
into #mL_min
FROM   
   (SELECT [IDNO]
	--, S5GFR
	,S5GFRM2
	,S5GFR_EPI

   FROM omoprawdata.dbo.S5ALL23 ) p  
UNPIVOT  
   (Lab_value FOR Lab_name IN   

     (--S5GFR
	  S5GFRM2
	  ,S5GFR_EPI )  
)AS unpvt;  
GO


--add the measurement date
update mg set Lab_date = rw.S5exdate	
, Phase = 'Phase V'
 from #mL_min mg
join  omoprawdata.dbo.S5ALL23   rw on rw.idno = mg.idno 

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
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	from #mL_min pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
)a
---------------------------

-- Unpivot the source table. (unit % ) 
if object_id('tempdb.dbo.#percen') is not null drop table #percen 
SELECT IDNO, Lab_name, Lab_value, 
	cast(NULL as datetime) as Lab_date,  
	cast(NULL as varchar(10)) as Phase
into #percen
FROM   
   (SELECT [IDNO]
    , S5HCT
	, S5NEUT
	--, S5BDFAT
	, S5HBA1C	

  FROM[omoprawdata].dbo.S5ALL23 ) p  
UNPIVOT  
   (Lab_value FOR Lab_name IN   
      (
		  S5HCT
		, S5NEUT
		--, S5BDFAT
		, S5HBA1C )  
)AS unpvt;  
GO


--add the measurement date
update mg set Lab_date = rw.S5exdate
, Phase = 'Phase V'
 from #percen mg
join [omoprawdata].dbo.S5ALL23 rw on rw.idno = mg.idno 

select * from #percen


---- Insert SHS Phase I, II, III Labs data (with unit %  )
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
, 8554							as unit_concept_id    --	%
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, '%'							as unit_source_value
, a.measurement_value			as value_source_value
from (
	select pat.[IDNO]						as person_id
	, pat.Lab_date							as measurement_date
	, Lab_name								as measurement_source_value
	, Lab_value								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	from #percen pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
)a

---------------------------


-- Unpivot the source table. (unit mg/g ) 
if object_id('tempdb.dbo.#mg_g') is not null drop table #mg_g
SELECT IDNO, Lab_name, Lab_value, 
	cast(NULL as datetime) as Lab_date,  
	cast(NULL as varchar(10)) as Phase
into #mg_g
FROM   
   (SELECT [IDNO]
	, S5UACR

   FROM [omoprawdata].dbo.S5ALL23 ) p  
UNPIVOT  
   (Lab_value FOR Lab_name IN   
      (S5UACR )  
)AS unpvt;  
GO


--add the measurement date
update mg set Lab_date = rw.S5exdate
, Phase = 'Phase V'
 from #mg_g mg
join [omoprawdata].dbo.S5ALL23 rw on rw.idno = mg.idno 

select * from #mg_g



---- Insert SHS Phase I, II, III Labs data (with unit %  )
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
, 8723							as unit_concept_id    --		milligram per gram
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, 'mg/g'						as unit_source_value
, a.measurement_value			as value_source_value

from (
	select pat.[IDNO]						as person_id
	, pat.Lab_date							as measurement_date
	, Lab_name								as measurement_source_value
	, Lab_value								as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	from #mg_g pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.Lab_name
)a

---------------------------


