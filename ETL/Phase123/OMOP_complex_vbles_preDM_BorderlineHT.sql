/********************************************************************
Name: ETL to populate OMOP Measurement Table

Source Data tables: SHS Phase123
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
	select idno , S1EXDATE, S2EXDATE, S3EXDATE, vble, value
	into #vbles
	from (	
		select IDNO, S1EXDATE, S2EXDATE, S3EXDATE
		--, S1ADADM, S2ADADM, S3ADADM
		--, cast( S1ADADMD as varchar(50)) S1ADADMD, cast( s2ADADMD as varchar(50)) s2ADADMD ,cast(  S3ADADMD as varchar(50)) S3ADADMD
		--, cast( S1CKD as varchar(50)) S1CKD, cast( S2CKD as varchar(50)) S2CKD, cast( S3CKD as varchar(50)) S3CKD
		--, S1CKDSTAGE, S2CKDSTAGE, S3CKDSTAGE
		--, S1DMTX, S2DMTX, S3DMTX2
		--, S1HTNRX2, S2HTNRX3, S3HTNRX
		--, S1KIDTRA, S2KIDTRA, S3KIDTRA
		--, S1WHNDMD, S2WHNDMD, S3WHNDMD	
		--, S1WHODMD, S2WHODMD, S3WHODMD	--check mapping
		, S1USHTN2, S2USHTN2, S3USHTN
		, S1WHOHN2, S2WHOHN2, S3WHOHTN	
		, S1WHODM, S2WHODM, S3WHODM	
		, S1WHNDM, S2WHNDM, S3WHNDM	
		from omoprawdata.dbo.SHSALL33 shs
	) p
	unpivot (
		value for vble in (	
		-- S1ADADM, S2ADADM, S3ADADM
		--, S1ADADMD,  s2ADADMD,  S3ADADMD
		--,  S1CKD,  S2CKD,  S3CKD
		--, S1CKDSTAGE, S2CKDSTAGE, S3CKDSTAGE
		--, S1DMTX, S2DMTX, S3DMTX2
		--, S1HTNRX2, S2HTNRX3, S3HTNRX
		--, S1KIDTRA, S2KIDTRA, S3KIDTRA
		--, S1WHNDMD, S2WHNDMD, S3WHNDMD	
		--, S1WHODMD, S2WHODMD, S3WHODMD	
		  S1USHTN2, S2USHTN2, S3USHTN
		, S1WHOHN2, S2WHOHN2, S3WHOHTN	
		, S1WHODM, S2WHODM, S3WHODM	
		, S1WHNDM, S2WHNDM, S3WHNDM	
		)
	) as unpvt;	

	
	select * from #vbles 
--------------------------------------------------
	
/* SHS variable
--WHOHN: SHS3 HYPERTENSION BY WHO DEFINITION	
Y = HYPERTENSION
B = BORDERLINE HYPERTENSION
N = NORMOTENSIVE
*/

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

variable: WHODM	2000000003	DIABETES STATUS ACCORDING TO 1985 WHO CRITERIA	
DM = KNOWN DIABETES
IGT = IMPAIRED GLUCOSE TOLERANCE
NGT = NORMAL GLUCOSE TOLERANCE

variable: WHNDM	2000000004	 DIABETES STATUS ACCORDING TO 1998 WHO CRITERIA	
DM = KNOWN DIABETES
IFG = IMPAIRED FASTING GLUCOSE TOLERANCE
IGT = IMPAIRED GLUCOSE TOLERANCE
NGT = NORMAL GLUCOSE TOLERANCE

variable: USHTN	2000000005 HYPERTENSION BY US DEFINITION
Y
N 

*/

--------------------------------------------------

--
begin 
	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--141693	Elevated blood-pressure reading without diagnosis of hypertension	Observation	SNOMED	Clinical Finding	S	371622005	1970-01-01	2099-12-31	NULL
	----4201478	Borderline blood pressure	Condition	SNOMED	Clinical Finding	S	314956000	1970-01-01	2099-12-31	NULL
	select 'S1WHOHN2_B', 0, 'SHS', 4201478, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2WHOHN2_B', 0, 'SHS', 4201478, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3WHOHTN_B', 0, 'SHS', 4201478, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	--		----

	--37018196	Prediabetes	Condition	SNOMED	Clinical Finding	S	714628002	2016-01-31	2099-12-31	NULL
	--4311629	Impaired glucose tolerance	Condition	SNOMED	Clinical Finding	S	9414007	1970-01-01	2099-12-31	NULL
	select 'S1WHODM_IGT', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2WHODM_IGT', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3WHODM_IGT', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
		
	select 'S1WHNDM_IFG', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2WHNDM_IFG', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3WHNDM_IFG', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)

	select 'S1WHNDM_IGT', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2WHNDM_IGT', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3WHNDM_IGT', 0, 'SHS', 4311629, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
		----

	--4055678	Glucose tolerance test normal	Condition	SNOMED	Clinical Finding	S	166926006	1970-01-01	2099-12-31	NULL
	select 'S1WHODM_NGT', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2WHODM_NGT', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3WHODM_NGT', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
		
	select 'S1WHNDM_NGT', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S2WHNDM_NGT', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31' union all -- condition mapping (standard concept)
	select 'S3WHNDM_NGT', 0, 'SHS', 4055678, 'SNOMED', '1996-09-08',	'2099-12-31'  -- condition mapping (standard concept)

end
--------------------------------------------------



-- ETL standard concept mapping into the condition table
insert into omop1.dbo.condition_occurrence( 
condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_start_datetime, condition_end_date, condition_end_datetime, condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, condition_status_concept_id, condition_status_source_value
)
select next Value for  dbo.condition_seq as [condition_occurrence_id]
, IDNO [person_id]
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
, concat(vble, ' = ', [value]) condition_source_value
, 0 condition_source_concept_id
, 0 condition_status_concept_id
, NULL condition_status_source_value
from #vbles v 
left join omop1.dbo.source_to_concept_map c on concat(v.vble, '_', ltrim(rtrim(replace(v.[value], ' ', '')))) = c.source_code and c.target_vocabulary_id != 'SHS'
left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = v.[IDNO] and vo1.visit_source_value like 'SHS Phase I %'
left join omop1.dbo.visit_occurrence vo2 on vo2.person_id = v.[IDNO] and vo2.visit_source_value like 'SHS Phase II %'
left join omop1.dbo.visit_occurrence vo3 on vo3.person_id = v.[IDNO] and vo3.visit_source_value like 'SHS Phase III %'
where ( v.vble in ( 'S1WHOHN2', 'S2WHOHN2', 'S3WHOHTN') and v.[value] = 'B' ) -- only people with value = 'B' for borderline hypertension
or ( v.vble in ('S1WHODM', 'S2WHODM', 'S3WHODM') and (v.[value] like '%IGT%' or v.[value] like '%NGT%') )	-- only prediabetic pats
or ( v.vble in ('S1WHNDM', 'S2WHNDM', 'S3WHNDM') and (v.[value] like '%IGT%' or v.[value] like '%NGT%' or v.[value] like '%IFG%') ) -- only prediabetic pats

	---------------------------
	


--test

	--SELECT IDNO, S1EXDATE, S2EXDATE, S3EXDATE, S1WHOHN2, S2WHOHN2, S3WHOHTN
	--FROM omoprawdata.DBO.SHSALL33 WHERE S1WHOHN2 IS NOT NULL OR S2WHOHN2 IS NOT NULL OR S3WHOHTN IS NOT NULL 

	-- source
	select 'Source - I' Phase, 'S1WHOHN2' [vble], S1WHOHN2, count(*) from omoprawdata.dbo.SHSALL33 where S1WHOHN2 is not null group by S1WHOHN2 union 
	select 'Source - II' Phase, 'S2WHOHN2' [vble], S2WHOHN2, count(*) from omoprawdata.dbo.SHSALL33 where S1WHOHN2 is not null group by S2WHOHN2 union 
	select 'Source - III' Phase, 'S3WHOHTN' [vble], S3WHOHTN, count(*) from omoprawdata.dbo.SHSALL33 where S1WHOHN2 is not null group by S3WHOHTN


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.condition_source_value value_as_string, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (320128, 4201478)	--WHOHTN = Yes
	and condition_source_value like '%whoh%%'
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.condition_source_value

	
	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase, o.value_as_string, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000002	--WHOHTN
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.value_as_string
	order by phase, value_as_string



	
	-- source
	select 'I' Phase, 'S1WHODM' [vble], S1WHODM, count(*) from omoprawdata.dbo.SHSALL33 where S1WHODM is not null group by S1WHODM union
	select 'II' Phase, 'S2WHODM' [vble], S2WHODM, count(*) from omoprawdata.dbo.SHSALL33  where S2WHODM is not null group by S2WHODM union 
	select 'III' Phase, 'S3WHODM' [vble], S3WHODM, count(*) from omoprawdata.dbo.SHSALL33 where S3WHODM is not null  group by S3WHODM

	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, right(o.condition_source_value,3) value_as_string, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (201820, 4311629, 4055678) --WHODM = NGT, IGT, IFG
	and condition_source_value like '%who%'
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), right(o.condition_source_value,3)
	order by phase, value_as_string

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase, o.value_as_string, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000003	--WHODM
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.value_as_string
	order by phase, value_as_string



	
	-- source
	select 'I' Phase, 'S1WHNDM' [vble], S1WHNDM, count(*) from omoprawdata.dbo.SHSALL33 where S1WHNDM is not null group by S1WHNDM union
	select 'II' Phase, 'S2WHNDM' [vble], S2WHNDM, count(*) from omoprawdata.dbo.SHSALL33  where S2WHNDM is not null group by S2WHNDM union 
	select 'III' Phase, 'S3WHNDM' [vble], S3WHNDM, count(*) from omoprawdata.dbo.SHSALL33 where S3WHNDM is not null  group by S3WHNDM


	-- destination (standard code)
	select 'Standard Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, condition_source_value, count(*) counts 
	from omop1.dbo.condition_occurrence o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where condition_concept_id in (201820, 4311629, 4055678) --WHNDM = NGT, IGT, IFG
	and condition_source_value like '%whn%'
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), condition_source_value
	order by phase, condition_source_value

	-- destination (custom code) 
	select 'Custom Concepts' Concept_type, substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11) phase
	, o.observation_source_value, count(*) counts 
	from omop1.dbo.observation o 
	join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = o.visit_occurrence_id and vo.person_id = o.person_id
	where observation_concept_id = 2000000004	--WHNDM
	and left(vo.visit_source_value, 11) like 'SHS Phase I%' and left(vo.visit_source_value, 12) != 'SHS Phase IV'
	group by substring(vo.visit_source_value, 11,  CHARINDEX(' ', vo.visit_source_value, 11)-11), o.observation_source_value
	order by phase, observation_source_value


	----------------------------

	