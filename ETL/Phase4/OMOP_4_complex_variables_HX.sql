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
	select idno, S4EXDATE, vble, value
	into #vbles
	from (	
		select IDNO, S4EXDATE
		, S4DMHX
		, S4HTNHX
		from omoprawdata.dbo.S4ALL23 shs
	) p
	unpivot (
		value for vble in (	
		  S4DMHX
		, S4HTNHX
		)
	) as unpvt;	

	
	select * from #vbles 
--------------------------------------------------
	-- If mapping doesn't exists, data doesn't exist in table either
	drop table if exists #map_exists 
	select sc.*, c.domain_id  into #map_exists 
	from omop1.dbo.source_to_concept_map sc
		left join omop1.dbo.concept c on sc.target_concept_id = c.concept_id
		where source_code  in ( 'S4DMHX', 'S4HTNHX')

	select * from #map_exists



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
	----------------------------------


-- ETL DMHX into the observation table
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
	, a.value_as_concept_id			as value_as_concept_id
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
		, cast(pat.S4EXDATE as datetime)				as observation_date		
		, pat.vble
		, pat.[value]									as value_as_string
		, case
			when pat.[value] = 'Y' then 4188539 --Yes
			when pat.[value] = 'N' then 4188540 --No
			when pat.[value] = 'B' then 45880922 --Borderline
			when pat.[value] = 'I' then 45879977 --Impaired
			when pat.[value] = 'P' then 45885207 --Pregnancy
			else 0 
		  end											as value_as_concept_id
		, vo1.visit_occurrence_id						as visit_occurrence_id
		, concat(pat.vble, ' = ', pat.[value])			as observation_source_value
		from #vbles pat 
		left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble and shs1.target_vocabulary_id = 'SHS' 
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase IV %'
		where pat.vble = 'S4DMHX'
	)a

	--

	
-- ETL DMHX into the observation table
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
	, a.value_as_concept_id			as value_as_concept_id
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
		, cast(pat.S4EXDATE as datetime)				as observation_date		
		, pat.vble
		, pat.[value]									as value_as_string
		, case
			when pat.[value] = 'Y' then 4188539 --Yes
			when pat.[value] = 'N' then 4188540 --No
			when pat.[value] = 'B' then 45880922 --Borderline
			when pat.[value] = 'I' then 45879977 --Impaired
			when pat.[value] = 'P' then 45885207 --Pregnancy
			else 0 
		  end											as value_as_concept_id
		, vo1.visit_occurrence_id						as visit_occurrence_id
		, concat(pat.vble, ' = ', pat.[value])			as observation_source_value
		from #vbles pat 
		left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble and shs1.target_vocabulary_id = 'SHS' 
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase IV %'
		where pat.vble = 'S4HTNHX'
	)a

	---------------------------

	--testing
		
	-- source
	select 'IV' Phase, 'S4DMHX' [vble], S4DMHX, count(*) from omoprawdata.dbo.S4ALL23 where S4DMHX is not null group by S4DMHX 

	-- destination 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 4058709	-- DMHX
	and left(vo.visit_source_value, 12) like 'SHS Phase IV%' 
	group by substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12), o.observation_source_value
	order by phase, observation_source_value


	-- source
	select 'IV' Phase, 'S4HTNHX' [vble], S4HTNHX, count(*) from omoprawdata.dbo.S4ALL23 where S4HTNHX is not null group by S4HTNHX

	-- destination 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 4058286	-- HTNHX
	and left(vo.visit_source_value, 12) like 'SHS Phase IV%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12), o.observation_source_value
	order by phase, observation_source_value


