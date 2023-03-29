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
	-- If mapping doesn't exists, data doesn't exist in table either
	drop table if exists #map_exists 
	select sc.*, c.domain_id  into #map_exists 
	from omop1.dbo.source_to_concept_map sc
		left join omop1.dbo.concept c on sc.target_concept_id = c.concept_id
		where source_code in (
		  'S5USHTN', 'S5WHOHTN', 'S5ADADM'
		) 


	-- delete old mapping if mapping exists 
	begin transaction t1	
		if (select count(*)  from #map_exists	) >0
		begin 
			-- delete from data table
			delete o 
			from omop1.dbo.observation o 
			join #map_exists me on me.target_concept_id = o.observation_concept_id


			-- delete from mapping table
			delete scm 
			from omop1.dbo.source_to_concept_map scm 
			join #map_exists me on me.source_code = scm.source_code and me.target_concept_id = scm.target_concept_id 
		end 
		



	commit  transaction t1
--------------------------------------------------

/*
standard concept: 
---------------- 
320128	Essential hypertension	Condition	SNOMED	Clinical Finding	S	59621000	1970-01-01	2099-12-31	NULL
201820	Diabetes mellitus	Condition	SNOMED	Clinical Finding	S	73211009	1970-01-01	2099-12-31	NULL

custom concept: 
----------------

variable: WHOHTN 2000000002		HYPERTENSION BY WHO DEFINITION	
values: Y = HYPERTENSION (45877994	Yes	Meas Value	LOINC	Answer	S	LA33-6	1970-01-01	2099-12-31	NULL)
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

	--320128	Essential hypertension	Condition	SNOMED	Clinical Finding	S	59621000	1970-01-01	2099-12-31	NULL
	select 'S5WHOHTN', 0, 'SHS', 320128, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S5WHOHTN', 0, 'SHS', 2000000002, 'SHS', '1996-09-08',	'2099-12-31' union all --  observation mapping (custom concept)
	 ----

	select 'S5USHTN', 0, 'SHS', 320128, 'SNOMED', '1996-09-08',		'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3USHTN', 0, 'SHS', 2000000005, 'SHS', '1996-09-08',	'2099-12-31' union all --  observation mapping (custom concept)


	 --201820	Diabetes mellitus	Condition	SNOMED	Clinical Finding	S	73211009	1970-01-01	2099-12-31	NULL
	select 'S5ADADM', 0, 'SHS', 201820, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S5ADADM', 0, 'SHS', 2000000007, 'SHS', '1996-09-08',	'2099-12-31'  --  observation mapping (custom concept)	
		----
	
end
--------------------------------------------------


-- ETL standard concept mapping into the condition table
insert into omop1.dbo.condition_occurrence( 
condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
)
select next Value for  dbo.condition_seq as [condition_occurrence_id]
, IDNO [person_id]
, c.target_concept_id condition_concept_id
, S5EXDATE as [condition_start_date]
, S5EXDATE as [condition_start_datetime]
, NULL condition_end_date
, NULL condition_end_datetime
, 45905770 as condition_type_concept_id	--- Patient Self-Reported Condition
, vo1.visit_occurrence_id as visit_occurrence_id
, concat(vble, ' = ', [value]) condition_source_value
, 0 condition_source_concept_id
, 0 condition_status_concept_id
, NULL condition_status_source_value
from #vbles v 
left join omop1.dbo.source_to_concept_map c on v.vble = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
where ( v.vble in ( 'S5USHTN', 'S5WHOHTN') and v.[value] = 'Y' ) -- only people with value = 'Y' for hypertension
or ( v.vble in ('S5ADADM') and v.[value] in ('DM') )	-- only diabetic pats


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
		, cast(S5EXDATE as datetime)					as observation_date		
		, pat.vble
		, pat.[value]									as value_as_string
		, vo1.visit_occurrence_id						as visit_occurrence_id
		, concat(pat.vble, ' = ', pat.[value])			as observation_source_value
		from #vbles pat 
		left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble and shs1.target_vocabulary_id = 'SHS'
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
	)a
	---------------------------
	
--update value as concept id 
--45877994	Yes	Meas Value	LOINC	Answer	S	LA33-6	1970-01-01	2099-12-31	NULL
--45878245	No	Meas Value	LOINC	Answer	S	LA32-8	1970-01-01	2099-12-31	NULL
--45880922	Borderline	Meas Value	LOINC	Answer	S	LA4259-3	1970-01-01	2099-12-31	NULL

-- WHOHTN
	select o.observation_id, o.person_id, o.observation_concept_id, o.observation_datetime, value_as_string
	, value_as_concept_id, o.observation_source_value
	, c.concept_name, c.vocabulary_id, c.standard_concept
	from omop1.dbo.observation o
	left join omop1.dbo.concept c on o.value_as_concept_id = c.concept_id
	where observation_concept_id = 2000000002 -- HYPERTENSION BY WHO DEFINITION	
	and left(observation_source_value, 2) = 'S5'

	update omop1.dbo.observation
	set value_as_concept_id = case 
		when value_as_string like '%y%' then 45877994 --yes
		when value_as_string like '%n%' then 45878245 --no
		when value_as_string like '%b%' then 45880922 --borderline
		else value_as_concept_id
	end 
	where observation_concept_id = 2000000002 -- HYPERTENSION BY WHO DEFINITION	
	and left(observation_source_value, 2) = 'S5'


--update value as concept id 
--45877994	Yes	Meas Value	LOINC	Answer	S	LA33-6	1970-01-01	2099-12-31	NULL
--45878245	No	Meas Value	LOINC	Answer	S	LA32-8	1970-01-01	2099-12-31	NULL
--45880922	Borderline	Meas Value	LOINC	Answer	S	LA4259-3	1970-01-01	2099-12-31	NULL

	
-- USHTN
	select o.observation_id, o.person_id, o.observation_concept_id, o.observation_datetime, value_as_string
	, value_as_concept_id, o.observation_source_value
	, c.concept_name, c.vocabulary_id, c.standard_concept
	from omop1.dbo.observation o
	left join omop1.dbo.concept c on o.value_as_concept_id = c.concept_id
	where observation_concept_id = 2000000005 -- HYPERTENSION BY US DEFINITION
	and left(observation_source_value, 2) = 'S5'


	update omop1.dbo.observation
	set value_as_concept_id = case 
		when value_as_string like '%y%' then 45877994 --yes
		when value_as_string like '%n%' then 45878245 --no
		else value_as_concept_id
	end 
	where observation_concept_id = 2000000005 -- HYPERTENSION BY US DEFINITION
	and left(observation_source_value, 2) = 'S5'




	
-- ADADM

--21498446	Diabetic	Meas Value	LOINC	Answer	S	LA26134-9	1970-01-01	2099-12-31	NULL
--45879977	Impaired	Meas Value	LOINC	Answer	S	LA13035-3	1970-01-01	2099-12-31	NULL
--45884153	Normal	Meas Value	LOINC	Answer	S	LA6626-1	1970-01-01	2099-12-31	NULL

	select o.observation_id, o.person_id, o.observation_concept_id, o.observation_datetime, value_as_string
	, value_as_concept_id, o.observation_source_value
	, c.concept_name, c.vocabulary_id, c.standard_concept
	from omop1.dbo.observation o
	left join omop1.dbo.concept c on o.value_as_concept_id = c.concept_id
	where observation_concept_id = 	2000000007	 --ADADM DM STATUS, ADA CRITERIA	
	and left(observation_source_value, 2) = 'S5'


	update omop1.dbo.observation
	set value_as_concept_id = case 
		when value_as_string like '%DM%' then 21498446	--Diabetic
		when value_as_string like '%IFG%' then 45879977	--Impaired
		when value_as_string like '%NFG%' then  45884153 --Normal
		else value_as_concept_id
	end 
	where observation_concept_id = 2000000007	 --ADADM DM STATUS, ADA CRITERIA
	and left(observation_source_value, 2) = 'S5'

