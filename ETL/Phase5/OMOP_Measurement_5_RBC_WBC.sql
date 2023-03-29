use omoprawdata



-- List variables in Phase5 in the source SHS table
drop table if exists #vbles 
select idno , S5EXDATE, vble, [value]
	, cast('' as varchar(50)) as units, 0 as unit_concept_id
into #vbles
from (	
	select DISTINCT  IDNO, S5EXDATE
	, S5RBC, S5WBC
	from omoprawdata.dbo.S5ALL23 shs
) p	
unpivot (
	value for vble in (	
	S5RBC, S5WBC
	)
) as unpvt;	
	

---- Add units and unit_concept_ids
--rbc 10^12/L	(8734	trillion per liter	Unit	UCUM	Unit	S	10*12/L	1970-01-01	2099-12-31	NULL)
update #vbles set units = '10^12/L', unit_concept_id = 8734 where vble = 'S5RBC'

--wbc 10^9/L	(9444	billion per liter	Unit	UCUM	Unit	S	10*9/L	1970-01-01	2099-12-31	NULL)
update #vbles set units = '10^9/L', unit_concept_id = 9444 where vble = 'S5WBC'

select * from #vbles


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
	, pat.S5EXDATE							as measurement_date
	, pat.vble								as measurement_source_value
	, pat.[value]							as measurement_value
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, pat.units								as unit_source_value
	, pat.unit_concept_id					as unit_concept_id
	from #vbles pat 
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
	left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble
)a


----testing 

	-- source
	select 'V' Phase, 'S5RBC' [vble], S5RBC, count(*) from omoprawdata.dbo.S5ALL23 where S5RBC is not null group by S5RBC

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.value_source_value, count(*) counts 
	from omop1.dbo.measurement o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where measurement_concept_id = 3020416	--RBC
	and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.value_source_value
	order by phase, value_source_value

	-- source
	select 'V' Phase, 'S5WBC' [vble], S5WBC, count(*) from omoprawdata.dbo.S5ALL23 where S5WBC is not null group by S5WBC

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.value_source_value, count(*) counts 
	from omop1.dbo.measurement o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where measurement_concept_id = 3000905	--WBC
	and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.value_source_value
	order by phase, value_source_value
