/********************************************************************
Name: ETL to populate OMOP Measurement Table

Source Data tables: SHS Phase5
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE
Destination table: (Multiple)

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

Description: 
	StrongHeart data has multiple complex variables such as Hypertension as per US criteria 
	and Hypertension based on WHO criteria. This variable is mapped to a custom concept 
	to closely resemble the source data and it is also mapped to a SNOMED condition code 
	so that this data is discoverable by the larger OMOP community if the StrongHeart data 
	is used in multi-site studies using OMOP CDM
*********************************************************************/

	-- List complex variables in Phase123 in the source SHS table
	drop table if exists #vbles 
	select idno, S5EXDATE, vble, value
	into #vbles
	from (	
		select IDNO, S5EXDATE
		, S5DMTX
		, S5HTNRX
		from omoprawdata.dbo.S5ALL23 shs
	) p
	unpivot (
		value for vble in (	
		 S5DMTX
		, S5HTNRX
		)
	) as unpvt;	

	
	select * from #vbles 
--------------------------------------------------
	

/*
standard concept: 
---------------- 
-- Drug_exposure
--21600712	DRUGS USED IN DIABETES	Drug	ATC	ATC 2nd	C	A10	1970-01-01	2099-12-31	NULL 
--21600381	ANTIHYPERTENSIVES	Drug	ATC	ATC 2nd	C	C02	1970-01-01	2099-12-31	NULL


custom concept: 
----------------
DMTX	SHS DIABETES TREATMENT	2000000008
B = BOTH INSULIN AND ORAL AGENT
I = INSULING TREATMENT
O = ORAL AGENT
N = NONE


HTNRX	SHS1 HYPERTENTION TREATMENT	2000000009
Y = YES
N = NO



*/
--------------------------------------------------


begin 

	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

			--standard concepts
	--21600712	DRUGS USED IN DIABETES	Drug	ATC	ATC 2nd	C	A10	1970-01-01	2099-12-31	NULL 
	select 'S5DMTX_B', 0, 'SHS', 21600712, 'ATC', '1970-01-01',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S5DMTX_I', 0, 'SHS', 21600712, 'ATC', '1970-01-01',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S5DMTX_O', 0, 'SHS', 21600712, 'ATC', '1970-01-01',	'2099-12-31' union all -- condition mapping (standard concept)

	--21600381	ANTIHYPERTENSIVES	Drug	ATC	ATC 2nd	C	C02	1970-01-01	2099-12-31	NULL
	select 'S5HTNRX2_Y', 0, 'SHS', 21600381, 'ATC', '1970-01-01',	'2099-12-31' union all -- condition mapping (standard concept)

	---- custom concepts
	select 'S5DMTX', 0, 'SHS', 2000000008, 'SHS', '1970-01-01',	'2099-12-31' union all -- observation mapping (standard concept)
	select 'S5HTNRX2', 0, 'SHS', 2000000009, 'SHS', '1970-01-01',	'2099-12-31' -- observation mapping (standard concept)


end
--------------------------------------------------


--standard concepts DM treatment
insert into omop1.dbo.drug_exposure(
drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime, drug_type_concept_id
, visit_occurrence_id, drug_source_value, drug_source_concept_id, route_concept_id)
select next Value for  dbo.drug_exp_seq		as							[drug_exposure_id]
, IDNO																	[person_id]
, c.target_concept_id													[drug_concept_id]
, S5EXDATE																[drug_exposure_start_date]
, cast(S5EXDATE as datetime)											[drug_exposure_start_datetime]
, 44787730																[drug_type_concept_id]	--- Patient Self-Reported Medication	
, vo1.visit_occurrence_id												[visit_occurrence_id]
, concat(vble, ' = ',
case when  v.vble like '%DMTX%' and v.[value] = 'B' then 'B (BOTH INSULIN AND ORAL AGENT)'
	when v.vble like '%DMTX%' and v.[value] = 'N' then 'N (NONE)' 
	when v.vble like '%DMTX%' and v.[value] = 'O' then 'O (ORAL AGENT)'
	when v.vble like '%DMTX%' and v.[value] = 'I' then 'I (INSULING TREATMENT)'
	else v.[value]  
end)																	[drug_source_value]
, 0																		[drug_source_concept_id]
, 0																		[route_concept_id]
from #vbles v 
left join omop1.dbo.source_to_concept_map c on concat(v.vble, '_', ltrim(rtrim(replace(v.[value], ' ', '')))) = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
where  ( v.vble = 'S5DMTX'	and (v.[value] like '%B%' or v.[value] like '%I%' or v.[value] like '%O%' ))	-- only for pats w/ diabetic treatment





--standard concepts HTN treatment
insert into omop1.dbo.drug_exposure(
drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_start_datetime, drug_type_concept_id
, visit_occurrence_id, drug_source_value, drug_source_concept_id, route_concept_id)
select next Value for  dbo.drug_exp_seq		as							[drug_exposure_id]
, IDNO																	[person_id]
, c.target_concept_id													[drug_concept_id]
, S5EXDATE																[drug_exposure_start_date]
, cast(S5EXDATE as datetime)											[drug_exposure_start_datetime]
, 44787730																[drug_type_concept_id]	--- Patient Self-Reported Medication	
, vo1.visit_occurrence_id												[visit_occurrence_id]
, concat(vble, ' = ',
case when v.vble like '%HTNRX%' and v.[value] = 'Y' then 'Y (YES)'
	when v.vble like '%HTNRX%' and v.[value] = 'N' then 'N (NO)'
	else v.[value]  
end)																	[drug_source_value]
, 0																		[drug_source_concept_id]
, 0																		[route_concept_id]
from #vbles v 
left join omop1.dbo.source_to_concept_map c on concat(v.vble, '_', ltrim(rtrim(replace(v.[value], ' ', '')))) = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
where  ( v.vble = 'S5HTNRX' and v.[value] like '%Y%' )	-- only for pats w/ HTN treatment

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
	, 0								as value_as_concept_id
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
		, cast(pat.S5EXDATE as datetime)				as observation_date		
		, pat.vble
		,  case 
			when pat.vble like '%DMTX%' and pat.[value] = 'B' then 'B = BOTH INSULIN AND ORAL AGENT'
			when pat.vble like '%DMTX%' and pat.[value] = 'N' then 'N = NONE' 
			when pat.vble like '%DMTX%' and pat.[value] = 'O' then 'O = ORAL AGENT'
			when pat.vble like '%DMTX%' and pat.[value] = 'I' then 'I = INSULING TREATMENT'
			when pat.vble like '%HTNRX%' and pat.[value] = 'Y' then 'Y = YES'
			when pat.vble like '%HTNRX%' and pat.[value] = 'N' then 'N = NO'
			else PAT.[value]
		end												as value_as_string
		, case 
			when pat.vble like '%DMTX%' and pat.[value] = 'B' then 45883500	--Both
			when pat.vble like '%DMTX%' and pat.[value] = 'N' then 4124462 --None
			when pat.vble like '%DMTX%' and pat.[value] = 'O' then 45878097	--Oral
			when pat.vble like '%DMTX%' and pat.[value] = 'I' then 45884660	--Insulin
			when pat.vble like '%HTNRX%' and pat.[value] = 'Y' then 45877994 --yes
			when pat.vble like '%HTNRX%' and pat.[value] = 'N' then 45878245 --no
			else 0
		end												as value_as_concept_id
		, vo1.visit_occurrence_id						as visit_occurrence_id
		, concat(pat.vble, ' = ', pat.[value])			as observation_source_value
		from #vbles pat 
		left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble and shs1.target_vocabulary_id = 'SHS' 
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
	)a
	---------------------------


	--testing
	
	-- source
	select 'V' Phase, 'S5DMTX' [vble], S5DMTX, count(*) from omoprawdata.dbo.S5ALL23 where S5DMTX is not null  group by S5DMTX


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, drug_source_value, count(*) counts 
	from omop1.dbo.drug_exposure o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where drug_concept_id in (21600712) -- DMTX
	and drug_source_value like '%dmtx%'
	and left(vo.visit_source_value, 11) like 'SHS Phase V %' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), drug_source_value
	order by phase, drug_source_value

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000008	--DMTX
	and left(vo.visit_source_value, 11) like 'SHS Phase V %' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.observation_source_value
	order by phase, observation_source_value

	----	
	
	-- source
	select 'V' Phase, 'S5HTNRX' [vble], S5HTNRX, count(*) from omoprawdata.dbo.S5ALL23 where S5HTNRX is not null  group by S5HTNRX


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, drug_source_value, count(*) counts 
	from omop1.dbo.drug_exposure o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where drug_concept_id in (21600381) -- HTNRX
	and drug_source_value like '%htnrx%'
	and left(vo.visit_source_value, 11) like 'SHS Phase V %' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), drug_source_value
	order by phase, drug_source_value

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000009	--HTNRX
	and left(vo.visit_source_value, 11) like 'SHS Phase V %' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.observation_source_value
	order by phase, observation_source_value
	
	 
                                                                                     