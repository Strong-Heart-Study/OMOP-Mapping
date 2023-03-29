use omoprawdata 
go

	-- List complex variables in Phase123 in the source SHS table
	drop table if exists #vbles 
	select idno, S4EXDATE, vble, [value], cast('' as varchar(50)) [value_detailed]
	into #vbles
	from (	
		select IDNO, S4EXDATE, S4KF
		from omoprawdata.dbo.S4ALL23 shs
	) p
	unpivot (
		value for vble in (	
		  S4KF
		)
	) as unpvt;	

	

	--UPDATE detailed values for source_value in OMOP
	update #vbles	set value_detailed = 'Y' where [value] = 'Y' and vble in ( 'S4KF')
	update #vbles	set value_detailed = 'N' where [value] = 'N' and vble in ( 'S4KF')

	select * from #vbles 
	-----------------------

	-- If mapping doesn't exists, data doesn't exist in table either
	drop table if exists #map_exists 
	select sc.*, c.domain_id  into #map_exists 
	from omop1.dbo.source_to_concept_map sc
		left join omop1.dbo.concept c on sc.target_concept_id = c.concept_id
		where source_code in (
		  'S4KF', 'S5KF'
		) 

		
	-- delete old mapping if mapping exists 
	begin transaction t1	
		if (select count(*)  from #map_exists	) >0
		begin 
			-- delete from data table
			delete o 
			from omop1.dbo.condition_occurrence o 
			join #map_exists me on me.target_concept_id = o.condition_concept_id


			-- delete from mapping table
			delete scm 
			from omop1.dbo.source_to_concept_map scm 
			join #map_exists me on me.source_code = scm.source_code and me.target_concept_id = scm.target_concept_id 
		end 
	

	commit  transaction t1

	
--------------------------------------------------

begin transaction t3

	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--193782	End-stage renal disease	Condition	SNOMED	Clinical Finding	S	46177005	1970-01-01	2099-12-31	NULL
	select 'S4KF', 0, 'SHS', 193782, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S5KF', 0, 'SHS', 193782, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)

	-- 40769277	End-stage renal disease [Reported]	Observation	LOINC	Clinical Observation	S	66617-2	2011-06-21	2099-12-31	NULL
	select 'S4KF_Y', 0, 'SHS', 40769277, 'SHS', '1996-09-08',	'2099-12-31' union all -- observation mapping (custom concept)
	select 'S5KF_Y', 0, 'SHS', 40769277, 'SHS', '1996-09-08',	'2099-12-31' union all --  observation mapping (custom concept)	 
	select 'S4KF_N', 0, 'SHS', 40769277, 'SHS', '1996-09-08',	'2099-12-31' union all -- observation mapping (custom concept)
	select 'S5KF_N', 0, 'SHS', 40769277, 'SHS', '1996-09-08',	'2099-12-31'  --  observation mapping (custom concept)	 

	
commit transaction t3 
	--------------------------------------------------


	-- ETL standard concept mapping into the condition table
	insert into omop1.dbo.condition_occurrence( 
	condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
	)
	select next Value for  dbo.condition_seq as [condition_occurrence_id]
	, * FROM (SELECT DISTINCT 
		 IDNO [person_id]
		, shs1.target_concept_id condition_concept_id
		, S4EXDATE as [condition_start_date]
		, cast(S4EXDATE as datetime) as [condition_start_datetime]
		, NULL condition_end_date
		, NULL condition_end_datetime
		, 45905770	condition_type_concept_id	--- Patient Self-Reported Condition
		, vo1.visit_occurrence_id as visit_occurrence_id
		, concat(vble, ' = ', [value], ' (', value_detailed, ')' ) condition_source_value
		, 0 condition_source_concept_id
		, 0 condition_status_concept_id
		, NULL condition_status_source_value
		from #vbles v 
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase IV %'
		left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= v.vble and shs1.target_vocabulary_id != 'SHS'
		where (v.vble = 'S4KF'	and v.[value] = 'Y')
	) A


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
			, case when pat.[value] = 'Y' then 4188539 --Yes
				   when pat.[value] = 'N' then 4188540 --No
				end											as value_as_concept_id
			, vo1.visit_occurrence_id						as visit_occurrence_id
			, concat(pat.vble, ' = ', pat.[value])			as observation_source_value
			from #vbles pat 
			left join omop1.dbo.source_to_concept_map shs1 on concat(pat.vble, '_', ltrim(rtrim(replace(pat.[value], ' ', '')))) = shs1.source_code  
			left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase IV %'
			where pat.vble = 'S4KF'	AND PAT.[value] is not null 
		)a
		
		
---------
	-- source
	select 'V' Phase, 'S4KF' [vble], S4KF, count(*) from omoprawdata.dbo.S4ALL23 where S4KF is not null group by S4KF 

	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12) phase
	, condition_source_value, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (193782) --kf
	and condition_source_value like '%KF%'
	and left(vo.visit_source_value, 12) like 'SHS Phase IV%' 
	group by substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12), condition_source_value
	order by phase, condition_source_value

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 40769277	--KF
	and left(vo.visit_source_value, 12) like 'SHS Phase IV%' 
	group by substring(vo.visit_source_value, 12,  CHARINDEX(' ', vo.visit_source_value, 12)-12), o.observation_source_value
	order by phase, observation_source_value
	
---------