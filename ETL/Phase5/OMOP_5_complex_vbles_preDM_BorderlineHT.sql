/********************************************************************
Name: ETL to populate OMOP Measurement Table

Source Data tables: SHS Phase123
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE
Destination table: (Multiple)

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'omoprawdata.dbo.S5All' 
		to the appropriate table that contains SHS Phase123 data

	3) Change omop1.dbo.source_to_concept_map to the 
		appropriate db/table name.

	4) Identify units in the source data for Labs. Change the unit_source_value and 
		unit_concept_id accordingly. Use SQL below to identify correct unit

		==============================================================================
			Select * from omop_vocabulary.vocab51.concept where domain_id = 'Unit'
			and standard_concept = 'S'
		==============================================================================

Description: 
	StrongHeart data has multiple complex variables such as Hypertension as per US criteria 
	and Hypertension based on WHO criteria. This variable is mapped to a custom concept 
	to closely resemble the source data and it is also mapped to a SNOMED condition code 
	so that this data is discoverable by the larger OMOP community if the StrongHeart data 
	is used in multi-site studies using OMOP CDM
*********************************************************************/

use omoprawdata 
go

-- List complex variables in Phase123 in the source SHS table
	drop table if exists #vbles 
	select idno, S5EXDATE, vble, value
	into #vbles
	from (	
		select IDNO, S5EXDATE
		, S5USHTN
		, S5WHOHTN	
		, S5ADADM
		from omoprawdata.dbo.S5ALL23 shs
	) p
	unpivot (
		value for vble in (	
		  S5USHTN
		, S5WHOHTN	
		, S5ADADM
		)
	) as unpvt;	

	
	select * from #vbles 
--------------------------------------------------

/*
standard concept: 
---------------- 
320128	Essential hypertension	Condition	SNOMED	Clinical Finding	S	59621000	1970-01-01	2099-12-31	NULL
201820	Diabetes mellitus	Condition	SNOMED	Clinical Finding	S	73211009	1970-01-01	2099-12-31	NULL

custom concept: 
----------------

variable: WHOHTN 2000000002		HYPERTENSION BY WHO DEFINITION	
Y = HYPERTENSION (45877994	Yes	Meas Value	LOINC	Answer	S	LA33-6	1970-01-01	2099-12-31	NULL)
B = BORDERLINE HYPERTENSION (45880922	Borderline	Meas Value	LOINC	Answer	S	LA4259-3	1970-01-01	2099-12-31	NULL)
N = NORMOTENSIVE (36309167	No (confirmed by test)	Meas Value	LOINC	Answer	S	LA20147-7	1970-01-01	2099-12-31	NULL)

variable: USHTN	2000000005 HYPERTENSION BY US DEFINITION
Y
N 

variable: ADADM DM STATUS, ADA CRITERIA
DM = KNOWN DIABETES
IFG = IMPAIRED FASTING GLUCOSE TOLERANCE
NFG = NORMAL FASTING GLUCOSE TOLERANCE

*/
--------------------------------------------------


--
begin 
	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--141693	Elevated blood-pressure reading without diagnosis of hypertension	Observation	SNOMED	Clinical Finding	S	371622005	1970-01-01	2099-12-31	NULL
	--4201478	Borderline blood pressure	Condition	SNOMED	Clinical Finding	S	314956000	1970-01-01	2099-12-31	NULL
	select 'S5WHOHTN_B', 0, 'SHS', 4201478, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)


	--37018196	Prediabetes	Condition	SNOMED	Clinical Finding	S	714628002	2016-01-31	2099-12-31	NULL
	--4311629	Impaired glucose tolerance	Condition	SNOMED	Clinical Finding	S	9414007	1970-01-01	2099-12-31	NULL
	select 'S5ADADM_IFG', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	----

	--4055678	Glucose tolerance test normal	Condition	SNOMED	Clinical Finding	S	166926006	1970-01-01	2099-12-31	NULL
	select 'S5ADADM_NFG', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31' -- condition mapping (standard concept)

end
--------------------------------------------------



-- ETL standard concept mapping into the condition table
insert into omop1.dbo.condition_occurrence( 
condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
)
select next Value for  dbo.condition_seq as [condition_occurrence_id]
, IDNO							[person_id]
, c.target_concept_id			[condition_concept_id]
, S5EXDATE as					[condition_start_date]
, cast(S5EXDATE as datetime) as [condition_start_datetime]
, NULL							[condition_end_date]
, NULL							[condition_end_datetime]
, 45905770						[condition_type_concept_id]	--- Patient Self-Reported Condition
, vo1.visit_occurrence_id as	[visit_occurrence_id]
, concat(vble, ' = ', [value])	[condition_source_value]
, 0								[condition_source_concept_id]
, 0								[condition_status_concept_id]
, NULL							[condition_status_source_value]
from #vbles v 
left join omop1.dbo.source_to_concept_map c on concat(v.vble, '_', ltrim(rtrim(replace(v.[value], ' ', '')))) = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
where ( v.vble = 'S5ADADM' and (v.[value] like '%IFG%' or v.[value] like '%NFG%' ) )	-- only prediabetic pats

	---------------------------


--test
	-- source
	select 'Source - V' Phase, 'S5ADADM' [vble], S5ADADM, count(*) from omoprawdata.dbo.S5ALL23 where S5ADADM is not null group by S5ADADM

	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, right(o.condition_source_value,3) value_as_string, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (201820, 4311629, 4055678) --ADADM = NFG, IFG, DM
	and condition_source_value like '%ADADM%'
	--and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), right(o.condition_source_value,3)
	order by phase, value_as_string

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase, o.value_as_string, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000007	--ADADM
	and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.value_as_string
	order by phase, value_as_string


	----------------------------

	use omoprawdata
	
-- ETL standard concept mapping into the condition table
insert into omop1.dbo.condition_occurrence( 
condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
)
select next Value for  dbo.condition_seq as [condition_occurrence_id]
, IDNO							[person_id]
, c.target_concept_id			[condition_concept_id]
, S5EXDATE as					[condition_start_date]
, cast(S5EXDATE as datetime) as [condition_start_datetime]
, NULL							[condition_end_date]
, NULL							[condition_end_datetime]
, 45905770						[condition_type_concept_id]	--- Patient Self-Reported Condition
, vo1.visit_occurrence_id as	[visit_occurrence_id]
, concat(vble, ' = ', [value])	[condition_source_value]
, 0								[condition_source_concept_id]
, 0								[condition_status_concept_id]
, NULL							[condition_status_source_value]
from #vbles v 
left join omop1.dbo.source_to_concept_map c on concat(v.vble, '_', ltrim(rtrim(replace(v.[value], ' ', '')))) = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
where ( v.vble = 'S5WHOHTN' and (v.[value] like '%B%'  ) )	-- only prediabetic pats

	---------test ------------------

	-- source
	select 'Source - V' Phase, 'S5WHOHTN' [vble], S5WHOHTN, count(*) from omoprawdata.dbo.S5ALL23 where S5WHOHTN is not null group by S5WHOHTN


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.condition_source_value value_as_string, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (320128, 4201478)	--WHOHTN = Yes, Borderline
	and condition_source_value like '%whoh%'
	and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.condition_source_value

	
	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase, o.value_as_string, count(*) counts 
	from omop1.dbo.observation o 
	left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000002	--WHOHTN
	and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.value_as_string
	order by phase, value_as_string


