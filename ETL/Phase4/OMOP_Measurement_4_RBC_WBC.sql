use omoprawdata



-- List variables in Phase5 in the source SHS table
drop table if exists #vbles 
select idno , S4EXDATE, vble, [value]
	, cast('' as varchar(50)) as units, 0 as unit_concept_id
into #vbles
from (	
	select DISTINCT  IDNO, S4EXDATE
	, S4RBC, S4WBC,S4ALB
	from omoprawdata.dbo.S4ALL23 shs
) p	
unpivot (
	value for vble in (	
	S4RBC, S4WBC, S4ALB
	)
) as unpvt;	
	

---- Add units and unit_concept_ids
--rbc 10^12/L	(8734	trillion per liter	Unit	UCUM	Unit	S	10*12/L	1970-01-01	2099-12-31	NULL)
update #vbles set units = '10^12/L', unit_concept_id = 8734 where vble = 'S4RBC'

--wbc 10^9/L	(9444	billion per liter	Unit	UCUM	Unit	S	10*9/L	1970-01-01	2099-12-31	NULL)
update #vbles set units = '10^9/L', unit_concept_id = 9444 where vble = 'S4WBC'

--8713	gram per deciliter	Unit	UCUM	Unit	S	g/dL	1970-01-01	2099-12-31	NULL
update #vbles set units = 'gm/dL', unit_concept_id = 8713 where vble = 'S4ALB'


select * from #vbles


-------------

begin transaction t1
	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--3020416	Erythrocytes [#/volume] in Blood by Automated count	Measurement	LOINC	Lab Test	S	789-8	1970-01-01	2099-12-31	NULL
	select 'S4RBC', 0, 'SHS', 3020416, 'SNOMED', '1996-09-08',	'2099-12-31' union all 
	select 'S5RBC', 0, 'SHS', 3020416, 'SNOMED', '1996-09-08',	'2099-12-31' union all 

	--3000905	Leukocytes [#/volume] in Blood by Automated count	Measurement	LOINC	Lab Test	S	6690-2	1970-01-01	2099-12-31	NULL
	select 'S4WBC', 0, 'SHS', 3000905, 'SNOMED', '1996-09-08',	'2099-12-31' union all  
	select 'S5WBC', 0, 'SHS', 3000905, 'SNOMED', '1996-09-08',	'2099-12-31' union all 

	--3024561	Albumin [Mass/volume] in Serum or Plasma	Measurement	LOINC	Lab Test	S	1751-7	1970-01-01	2099-12-31	NULL
	select 'S4ALB', 0, 'SHS', 3024561, 'SNOMED', '1996-09-08',	'2099-12-31'


commit transaction t1


-------------


---- Insert SHS Phase IV
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
, a.unit_concept_id				as unit_concept_id   
, NULL							as range_low
, NULL							as range_high
, 1								as provider_id
, a.visit_occurrence_id			as visit_occurrence_id
, NULl							as visit_detail_id
, a.measurement_source_value	as measurement_source_value
, 0								as measurement_source_concept_id
, a.unit_source_value			as unit_source_value
, a.measurement_value			as value_source_value
from (
	select pat.[IDNO]						as person_id
	, pat.S4EXDATE							as measurement_date
	, pat.vble								as measurement_source_value
	, pat.[value]							as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, pat.units								as unit_source_value
	, pat.unit_concept_id					as unit_concept_id
	from #vbles pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase IV %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble
)a


----testing 

	-- source
	select 'IV' Phase, 'S4RBC' [vble], S4RBC, count(*) from omoprawdata.dbo.S4ALL23 where S4RBC is not null group by S4RBC

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 12)-12) phase
	, o.value_source_value, count(*) counts 
	from omop1.dbo.measurement o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where measurement_concept_id = 3020416	--RBC
	and left(vo.visit_source_value, 12) like 'SHS Phase IV%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 12)-12), o.value_source_value
	order by phase, value_source_value

	-- source
	select 'IV' Phase, 'S4WBC' [vble], S4WBC, count(*) from omoprawdata.dbo.S4ALL23 where S4WBC is not null group by S4WBC

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 12)-12) phase
	, o.value_source_value, count(*) counts 
	from omop1.dbo.measurement o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where measurement_concept_id = 3000904	--WBC
	and left(vo.visit_source_value, 12) like 'SHS Phase IV%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 12)-12), o.value_source_value
	order by phase, value_source_value

	
	-- source
	select 'IV' Phase, 'S4ALB' [vble], S4ALB, count(*) from omoprawdata.dbo.S4ALL23 where S4ALB is not null group by S4ALB

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 12)-12) phase
	, o.value_source_value, count(*) counts 
	from omop1.dbo.measurement o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where measurement_concept_id = 40757478	--WBC
	and left(vo.visit_source_value, 12) like 'SHS Phase IV%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 12)-12), o.value_source_value
	order by phase, value_source_value
