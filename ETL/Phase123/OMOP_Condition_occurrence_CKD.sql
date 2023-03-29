use omoprawdata 
go

/*
SELECT S1CKDSTAGE, COUNT(*) COUNTS
FROM omoprawdata.dbo.SHSALL33
group by S1CKDSTAGE

ALTER TABLE  omoprawdata.dbo.SHSALL33 ALTER COLUMN S1CKDSTAGE FLOAT NULL;
*/


	-- List complex variables in Phase123 in the source SHS table
	drop table if exists #vbles 
	select idno , S1EXDATE, S2EXDATE, S3EXDATE, vble, [value], cast('' as varchar(50)) [value_detailed]
	into #vbles
	from (	
		select IDNO, S1EXDATE, S2EXDATE, S3EXDATE
		, S1CKD, S2CKD, S3CKD
		, S1CKDSTAGE, S2CKDSTAGE, S3CKDSTAGE
		from omoprawdata.dbo.SHSALL33 shs
	) p
	unpivot (
		value for vble in (	
		S1CKD, S2CKD, S3CKD
		, S1CKDSTAGE, S2CKDSTAGE, S3CKDSTAGE
		)
	) as unpvt;	
	

	-- Add detailed explanation of categorical values
	update #vbles	set value_detailed = 'Yes' where [value] = 1 and vble in ( 'S1CKD', 'S2CKD', 'S3CKD')
	update #vbles	set value_detailed = 'No' where [value] = 0 and vble in ( 'S1CKD', 'S2CKD', 'S3CKD')

	update #vbles	set value_detailed = 'CKD STAGE 1' where [value] = 1 and vble in ( 'S1CKDSTAGE','S2CKDSTAGE','S3CKDSTAGE')
	update #vbles	set value_detailed = 'CKD STAGE 2' where [value] = 2 and vble in ( 'S1CKDSTAGE','S2CKDSTAGE','S3CKDSTAGE')
	update #vbles	set value_detailed = 'CKD STAGE 3' where [value] = 3 and vble in ( 'S1CKDSTAGE','S2CKDSTAGE','S3CKDSTAGE')
	update #vbles	set value_detailed = 'CKD STAGE 4' where [value] = 4 and vble in ( 'S1CKDSTAGE','S2CKDSTAGE','S3CKDSTAGE')
	update #vbles	set value_detailed = 'CKD STAGE 5' where [value] = 5 and vble in ( 'S1CKDSTAGE','S2CKDSTAGE','S3CKDSTAGE')


	select * from #vbles 

	-----------------------

	-- If mapping doesn't exists, data doesn't exist in table either
	drop table if exists #map_exists 
	select sc.*, c.domain_id  into #map_exists 
	from omop1.dbo.source_to_concept_map sc
		left join omop1.dbo.concept c on sc.target_concept_id = c.concept_id
		where source_code in (
		  'S1CKD', 'S2CKD', 'S3CKD'	
		   ,'S1CKDSTAGE','S2CKDSTAGE','S3CKDSTAGE'
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

	--------------

	
--
begin transaction t3
	-- Custom concept 
	insert into omop1.dbo.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date)
	select 2000000010,  'SHS CHRONIC KIDNEY DISEASE, ALBUMINURIA or eGFR<60', 'Observation', 'SHS', 'SHS', NULL as standard_concept, 'CKD', '2020-10-01', '2099-12-31'

	---
	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--46271022	Chronic kidney disease	Condition	SNOMED	Clinical Finding	S	709044004	2015-07-31	2099-12-31	NULL
	select 'S1CKD', 0, 'SHS', 46271022, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2CKD', 0, 'SHS', 46271022, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3CKD', 0, 'SHS', 46271022, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S5CKD', 0, 'SHS', 46271022, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)

	-- custom concept id for CKD
	select 'S1CKD', 0, 'SHS', 2000000010, 'SHS', '1996-09-08',	'2099-12-31' union all -- observation mapping (custom concept)
	select 'S2CKD', 0, 'SHS', 2000000010, 'SHS', '1996-09-08',	'2099-12-31' union all --  observation mapping (custom concept)
	select 'S3CKD', 0, 'SHS', 2000000010, 'SHS', '1996-09-08',	'2099-12-31' union all --  observation mapping (custom concept)	 
	select 'S5CKD', 0, 'SHS', 2000000010, 'SHS', '1996-09-08',	'2099-12-31' union all --  observation mapping (custom concept)	 

	-- 443614 STAGE 1 (CKDSTAGE)
	select 'S1CKDSTAGE_1', 0, 'SHS', 443614, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2CKDSTAGE_1', 0, 'SHS', 443614, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3CKDSTAGE_1', 0, 'SHS', 443614, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S4CKDSTAGE_1', 0, 'SHS', 443614, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S5CKDSTAGE_1', 0, 'SHS', 443614, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)

	-- 443601 STAGE 2 (CKDSTAGE)
	select 'S1CKDSTAGE_2', 0, 'SHS', 443601, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2CKDSTAGE_2', 0, 'SHS', 443601, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3CKDSTAGE_2', 0, 'SHS', 443601, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S4CKDSTAGE_2', 0, 'SHS', 443601, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S5CKDSTAGE_2', 0, 'SHS', 443601, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)

	-- 443597 STAGE 3 (CKDSTAGE)
	select 'S1CKDSTAGE_3', 0, 'SHS', 443597, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2CKDSTAGE_3', 0, 'SHS', 443597, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3CKDSTAGE_3', 0, 'SHS', 443597, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S4CKDSTAGE_3', 0, 'SHS', 443597, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S5CKDSTAGE_3', 0, 'SHS', 443597, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)

	-- 443612 STAGE 4 (CKDSTAGE)
	select 'S1CKDSTAGE_4', 0, 'SHS', 443612, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2CKDSTAGE_4', 0, 'SHS', 443612, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3CKDSTAGE_4', 0, 'SHS', 443612, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S4CKDSTAGE_4', 0, 'SHS', 443612, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S5CKDSTAGE_4', 0, 'SHS', 443612, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)

	-- 443611 STAGE 5 (CKDSTAGE)
	select 'S1CKDSTAGE_5', 0, 'SHS', 443611, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2CKDSTAGE_5', 0, 'SHS', 443611, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3CKDSTAGE_5', 0, 'SHS', 443611, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S4CKDSTAGE_5', 0, 'SHS', 443611, 'SNOMED', '1996-09-08',	'2099-12-31' union all  -- condition mapping (standard concept)
	select 'S5CKDSTAGE_5', 0, 'SHS', 443611, 'SNOMED', '1996-09-08',	'2099-12-31'   -- condition mapping (standard concept)


commit transaction t3 


--------------


	-- ETL standard concept mapping into the condition table
	insert into omop1.dbo.condition_occurrence( 
	condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
	)
	select next Value for  dbo.condition_seq as [condition_occurrence_id]
	, * FROM (SELECT DISTINCT 
		 IDNO [person_id]
		, c.target_concept_id condition_concept_id
		, case when left(vble, 2) = 's1' then S1EXDATE
			when left(vble, 2) = 's2' then S2EXDATE
			when left(vble, 2) = 's3' then S3EXDATE
			end as [condition_start_date]
		, case when left(vble, 2) = 's1' then cast(S1EXDATE as datetime)
			when left(vble, 2) = 's2' then  cast(S2EXDATE as datetime)
			when left(vble, 2) = 's3' then  cast(S3EXDATE as datetime)
			end as [condition_start_datetime]
		, NULL condition_end_date
		, NULL condition_end_datetime
		, 45905770	condition_type_concept_id	--- Patient Self-Reported Condition
		, case 
			when left(vble, 2) = 'S1' then vo1.visit_occurrence_id
			when left(vble, 2) = 'S2' then vo2.visit_occurrence_id
			when left(vble, 2) = 'S3' then vo3.visit_occurrence_id	
		  end as visit_occurrence_id
		, concat(vble, ' = ', [value], ' (', value_detailed, ')' ) condition_source_value
		, 0 condition_source_concept_id
		, 0 condition_status_concept_id
		, NULL condition_status_source_value
		from #vbles v 
		left join omop1.dbo.source_to_concept_map c on v.vble = c.source_code and c.target_vocabulary_id != 'SHS'
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase I %'
		left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = v.[IDNO] and vo2.visit_source_value like 'SHS Phase II %'
		left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = v.[IDNO] and vo3.visit_source_value like 'SHS Phase III %'
		where v.vble in ( 'S1CKD', 'S2CKD', 'S3CKD')
		and v.[value] = 1	--People with CKD=1 in the condition_occurrence table
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
			, case when left(vble, 2) = 'S1' then cast(pat.S1EXDATE as datetime)
					when left(vble, 2) = 'S2' then cast(pat.S2EXDATE as datetime)
					when left(vble, 2) = 'S3' then cast(pat.S3EXDATE as datetime)
					end										as observation_date		
			, pat.vble
			, pat.[value]									as value_as_string
			, case when pat.[value] = '1' then 4188539 --Yes
				   when pat.[value] = '0' then 4188540 --No
				end											as value_as_concept_id
			, case 
				when left(vble, 2) = 'S1' then vo1.visit_occurrence_id
				when left(vble, 2) = 'S2' then vo2.visit_occurrence_id
				when left(vble, 2) = 'S3' then vo3.visit_occurrence_id
			  end											as visit_occurrence_id
			, concat(pat.vble, ' = ', pat.[value], ' (', pat.value_detailed, ')')			as observation_source_value
			from #vbles pat 
			left join omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.vble and shs1.target_vocabulary_id = 'SHS' 
			left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] and vo1.visit_source_value like 'SHS Phase I %'
			left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = pat.[IDNO] and vo2.visit_source_value like 'SHS Phase II %'
			left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = pat.[IDNO] and vo3.visit_source_value like 'SHS Phase III %'
			where pat.vble in ('S1CKD','S2CKD','S3CKD')
		)a
		---------------------------

	-- ETL standard concept mapping into the condition table
	insert into omop1.dbo.condition_occurrence( 
	condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
	)
	select next Value for  dbo.condition_seq as [condition_occurrence_id]
	, * FROM (SELECT DISTINCT 
		  IDNO [person_id]
		, c.target_concept_id condition_concept_id
		, case when left(vble, 2) = 's1' then S1EXDATE
			when left(vble, 2) = 's2' then S2EXDATE
			when left(vble, 2) = 's3' then S3EXDATE
			end as [condition_start_date]
		, case when left(vble, 2) = 's1' then cast(S1EXDATE as datetime)
			when left(vble, 2) = 's2' then  cast(S2EXDATE as datetime)
			when left(vble, 2) = 's3' then  cast(S3EXDATE as datetime)
			end as [condition_start_datetime]
		, NULL condition_end_date
		, NULL condition_end_datetime
		, 45905770	condition_type_concept_id	--- Patient Self-Reported Condition
		, case 
			when left(vble, 2) = 'S1' then vo1.visit_occurrence_id
			when left(vble, 2) = 'S2' then vo2.visit_occurrence_id
			when left(vble, 2) = 'S3' then vo3.visit_occurrence_id	
		  end as visit_occurrence_id
		, concat(vble, ' = ', [value], ' (', value_detailed, ')' ) condition_source_value
		, 0 condition_source_concept_id
		, 0 condition_status_concept_id
		, NULL condition_status_source_value
		from #vbles v 
		left join omop1.dbo.source_to_concept_map c on concat(v.vble, '_', ltrim(rtrim(replace(v.[value], ' ', '')))) = c.source_code and c.target_vocabulary_id != 'SHS'
		left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase I %'
		left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = v.[IDNO] and vo2.visit_source_value like 'SHS Phase II %'
		left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = v.[IDNO] and vo3.visit_source_value like 'SHS Phase III %'
		where v.vble in ( 'S1CKDSTAGE', 'S2CKDSTAGE', 'S3CKDSTAGE')
	) A
---------
	
	-- source
	select 'I' Phase, 'S1CKD' [vble], S1CKD, count(*) from omoprawdata.dbo.SHSALL33 where S1CKD is not null group by S1CKD union
	select 'II' Phase, 'S2CKD' [vble], S2CKD, count(*) from omoprawdata.dbo.SHSALL33  where S2CKD is not null group by S2CKD union 
	select 'III' Phase, 'S3CKD' [vble], S3CKD, count(*) from omoprawdata.dbo.SHSALL33 where S3CKD is not null  group by S3CKD


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, condition_source_value, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (46271022) --ckd
	and condition_source_value like '%CKD%'
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), condition_source_value
	order by phase, condition_source_value

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000010	--CKD
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.observation_source_value
	order by phase, observation_source_value

	
---------
	
	-- source
	select 'I' Phase, 'S1CKDSTAGE' [vble], S1CKDSTAGE, count(*) from omoprawdata.dbo.SHSALL33 where S1CKDSTAGE is not null group by S1CKDSTAGE union
	select 'II' Phase, 'S2CKDSTAGE' [vble], S2CKDSTAGE, count(*) from omoprawdata.dbo.SHSALL33  where S2CKDSTAGE is not null group by S2CKDSTAGE union 
	select 'III' Phase, 'S3CKDSTAGE' [vble], S3CKDSTAGE, count(*) from omoprawdata.dbo.SHSALL33 where S3CKDSTAGE is not null  group by S3CKDSTAGE


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, condition_source_value, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (443614, 443601, 443597, 443612, 443611) --ckdstage
	and condition_source_value like '%CKDSTAGE%'
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), condition_source_value
	order by phase, condition_source_value

	----------------------------

