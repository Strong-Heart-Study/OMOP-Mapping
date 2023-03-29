/********************************************************************
Name: ETL to populate OMOP Measurement Table

Source Data tables: SHS Phase6
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE
Destination table: (Multiple)

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'omoprawdata.dbo.S6ALL' 
		to the appropriate table that contains SHS Phase6 data

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
	select idno, S6EXDATE, vble, value
	into #vbles
	from (	
		select IDNO, convert(datetime, S6EXDATE, 121) S6EXDATE, S6ADADM
		from omoprawdata.dbo.s6all shs
	) p
	unpivot (
		value for vble in (S6ADADM)
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

	 --201820	Diabetes mellitus	Condition	SNOMED	Clinical Finding	S	73211009	1970-01-01	2099-12-31	NULL
	select 'S6ADADM', 0, 'SHS', 201820, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)

	--37018196	Prediabetes	Condition	SNOMED	Clinical Finding	S	714628002	2016-01-31	2099-12-31	NULL
	--4311629	Impaired glucose tolerance	Condition	SNOMED	Clinical Finding	S	9414007	1970-01-01	2099-12-31	NULL
	select 'S6ADADM_IFG', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)

	--4055678	Glucose tolerance test normal	Condition	SNOMED	Clinical Finding	S	166926006	1970-01-01	2099-12-31	NULL
	select 'S6ADADM_NF', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)

	----
	select 'S6ADADM', 0, 'SHS', 2000000007, 'SHS', '1996-09-08',	'2099-12-31'  --  observation mapping (custom concept)	

	
end
--------------------------------------------------


-- ETL standard concept mapping into the condition table
insert into omop1.dbo.condition_occurrence( 
condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
)
select next Value for  dbo.condition_seq as [condition_occurrence_id]
, IDNO [person_id]
, c.target_concept_id condition_concept_id
, S6EXDATE as [condition_start_date]
, cast(S6EXDATE as datetime) as [condition_start_datetime]
, NULL condition_end_date
, NULL condition_end_datetime
, 45905770	condition_type_concept_id	--- Patient Self-Reported Condition
, visit_occurrence_id as visit_occurrence_id
, concat(vble, ' = ', [value]) condition_source_value
, 0 condition_source_concept_id
, 0 condition_status_concept_id
, NULL condition_status_source_value
from #vbles v 
left join omop1.dbo.source_to_concept_map c on v.vble = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase VI %'
where ( v.vble = 'S6ADADM' and v.[value] = 'DM' )	-- only diabetic pats



-- ETL standard concept mapping into the condition table (IFG and NFG)
insert into omop1.dbo.condition_occurrence( 
condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
)
select next Value for  dbo.condition_seq as [condition_occurrence_id]
, IDNO [person_id]
, c.target_concept_id condition_concept_id
, S6EXDATE as [condition_start_date]
, cast(S6EXDATE as datetime) as [condition_start_datetime]
, NULL condition_end_date
, NULL condition_end_datetime
, 45905770	condition_type_concept_id	--- Patient Self-Reported Condition
, visit_occurrence_id as visit_occurrence_id
, concat(vble, ' = ', [value]) condition_source_value
, 0 condition_source_concept_id
, 0 condition_status_concept_id
, NULL condition_status_source_value
from #vbles v 
left join omop1.dbo.source_to_concept_map c on concat(v.vble, '_', ltrim(rtrim(replace(v.[value], ' ', '')))) = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase VI %'
where  ( v.vble = 'S6ADADM') and (v.[value] like '%IFG%' or v.[value] like '%NF%') 	--  prediabetic and normal pats

	---------------------------


-- ETL custom concept mapping into the observation table
insert into omop1.dbo.observation(
	observation_id, 
	person_id, 
	observation_concept_id, 
	observation_date, 
	observation_datetime, 
	observation_type_concept_id, 
	value_as_number, 
	value_as_string, 
	value_as_concept_id, 
	qualifier_concept_id, 
	unit_concept_id, 
	provider_id, 
	visit_occurrence_id, 
	visit_detail_id, 
	observation_source_value, 
	observation_source_concept_id,
	obs_event_field_concept_id
	  )
select next Value for  dbo.observation_seq as observation_id
	, a.person_id					as person_id
	, a.target_concept_id			as observation_concept_id
	, a.observation_date			as observation_date
	, a.observation_date			as observation_datetime
	, 45905771						as [observation_type_concept_id]  --	Observation Recorded from a Survey
	, NULL							as value_as_number
	, a.value_as_string				as value_as_string
	, a.value_as_concept			as value_as_concept_id
	, 0								as qualifier_concept_id    
	, 0								as unit_concept_id    
	, 1								as provider_id
	, a.visit_occurrence_id			as visit_occurrence_id
	, NULl							as visit_detail_id
	, a.observation_source_value	as observation_source_value
	, 0								as observation_source_concept_id
	, 0								as obs_event_field_concept_id
	from (
		select distinct pat.[IDNO]						as person_id
		, shs1.target_concept_id						as target_concept_id 
		, cast(pat.S6EXDATE as datetime)				as observation_date		
		, pat.vble
		, pat.[value]									as value_as_string
		, case when pat.value like '%DM%' then 21498446	--Diabetic
			when pat.value like '%IFG%' then 45879977	--Impaired
			when pat.value like '%NF%' then  45884153 --Normal
			end 										as value_as_concept
		, visit_occurrence_id							as visit_occurrence_id
		, concat(pat.vble, ' = ', pat.[value])			as observation_source_value
		from #vbles pat 
		left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble and shs1.target_vocabulary_id = 'SHS' 
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase VI %'
	)a
	---------------------------


	--testing
	
	-- source
	select 'VI' Phase, 'S6ADADM' [vble], S6ADADM, count(*) from omoprawdata.dbo.s6all where S6ADADM is not null  group by S6ADADM


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, condition_source_value, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (201820, 4311629, 4055678) --ADADM = NGT, IGT, IFG
	and condition_source_value like '%ada%'
	and left(vo.visit_source_value, 11) like 'SHS Phase VI %'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), condition_source_value
	order by phase, condition_source_value

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000007	--ADADM
	and left(vo.visit_source_value, 11) like 'SHS Phase VI %'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.observation_source_value
	order by phase, observation_source_value


