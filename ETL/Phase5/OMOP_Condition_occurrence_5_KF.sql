use omoprawdata 
go

	-- List complex variables in Phase123 in the source SHS table
	drop table if exists #vbles 
	select idno, S5EXDATE, vble, [value], cast('' as varchar(50)) [value_detailed]
	into #vbles
	from (	
		select IDNO, S5EXDATE, S5KF
		from omoprawdata.dbo.S5ALL23 shs
	) p	
	unpivot (
		value for vble in (	
		  S5KF
		)
	) as unpvt;	


	--UPDATE detailed values for source_value in OMOP
	update #vbles	set value_detailed = 'Y' where [value] = '1' and vble in ( 'S5KF')
	update #vbles	set value_detailed = 'N' where [value] = '2' and vble in ( 'S5KF')
	update #vbles	set value_detailed = 'Unknown' where [value] = '9' and vble in ( 'S5KF') --45877986	Unknown	Meas Value	LOINC	Answer	S	LA4489-6	1970-01-01	2099-12-31	NULL

	select * from #vbles 


	--update source_to_concept mapping 
begin transaction t5
	update omop1.dbo.source_to_concept_map set source_code = 'S5KF_1' where source_code = 'S5KF_Y'
	update omop1.dbo.source_to_concept_map set source_code = 'S5KF_2' where source_code = 'S5KF_N'

commit transaction t5
	-----------------------

	-- ETL standard concept mapping into the condition table
	insert into omop1.dbo.condition_occurrence( 
	condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
	)
	select next Value for  dbo.condition_seq as [condition_occurrence_id]
	, * FROM (SELECT DISTINCT 
		  IDNO [person_id]
		, shs1.target_concept_id condition_concept_id
		, S5EXDATE as [condition_start_date]
		, cast(S5EXDATE as datetime) as [condition_start_datetime]
		, NULL condition_end_date
		, NULL condition_end_datetime
		, 45905770	condition_type_concept_id	--- Patient Self-Reported Condition
		, vo1.visit_occurrence_id as visit_occurrence_id
		, concat(vble, ' = ', [value] ) condition_source_value
		, 0 condition_source_concept_id
		, 0 condition_status_concept_id
		, NULL condition_status_source_value
		from #vbles v 
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
		left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= v.vble and shs1.target_vocabulary_id != 'SHS'
		where v.vble = 'S5KF'	and v.[value] = '1'
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
			, cast(pat.S5EXDATE as datetime)				as observation_date		
			, pat.vble
			, pat.[value]									as value_as_string
			, case when pat.[value] = '1' then 4188539 --Yes
				   when pat.[value] = '2' then 4188540 --No
				   when pat.[value] = '9' then 45877986  --Unknown
				end											as value_as_concept_id
			, vo1.visit_occurrence_id						as visit_occurrence_id
			, concat(pat.vble, ' = ', pat.[value])			as observation_source_value
			from #vbles pat 
			left join omop1.dbo.source_to_concept_map shs1 on concat(pat.vble, '_', ltrim(rtrim(replace(pat.[value], ' ', '')))) = shs1.source_code  
			left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase V %'
			where pat.vble = 'S5KF'	AND PAT.[value] is not null 
		)a
		
		
---------
	-- source
	select 'V' Phase, 'S5KF' [vble], S5KF, count(*) from omoprawdata.dbo.S5ALL23 where S5KF is not null group by S5KF 

	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, condition_source_value, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (193782) --kf
	and condition_source_value like '%KF%'
	and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), condition_source_value
	order by phase, condition_source_value

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 40769277	--KF
	and left(vo.visit_source_value, 11) like 'SHS Phase V%' 
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.observation_source_value
	order by phase, observation_source_value
	
---------