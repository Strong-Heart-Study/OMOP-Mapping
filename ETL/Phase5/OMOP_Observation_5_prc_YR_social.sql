/********************************************************************
Name: ETL to populate OMOP Observation Table

Source Data tables: SHS Phase5
					SOURCE_TO_CONCEPT_MAP
					VISIT_OCCURRENCE_TABLE

Destination table: Observation

Instructions:
	1) Change the name of the DB to the appropriate database

	2) Change the name of the table 'omoprawdata.dbo.S5ALL23' 
		to the appropriate table that contains SHS Phase5 data

	3) Change dbo.source_to_concept_map to the 
		appropriate db/table name.

	4) Identify units in the source data for BP (Sys/Dias), 
		BMI and Body surface area. Change the unit_source_value and 
		unit_concept_id accordingly. Use SQL below to identify correct unit
		==============================================================================
			Select * from omop_vocabulary.vocab51.concept where domain_id = 'Unit'
			and standard_concept = 'S'
		==============================================================================

	5) range_low and range_high are currently set to 0. Please update these 
		values if they're available in source data.

		
*********************************************************************/

use omoprawdata
go


/** Note: The values aren't mapped to OMOP concepts due to very specific definitions. 
	Instead, value_as_string column is used to store the  values ***/
	


--check if data is already present 
select  observation_concept_id, left( observation_source_value , CHARINDEX('=', observation_source_value, 1) -2), count(*) counts 
from omop1.dbo.observation o 
join (
	select source_code, target_concept_id , c.domain_id
	from omop1.dbo.source_to_concept_map sc 
	join omop1.dbo.concept c on c.concept_id = sc.target_concept_id
	where source_code in (
	 'S5ESTRO'
	, 'S5ETOH'
	, 'S5ACR'
	--, 'S5ACR2' (same information as in S5ACR. present only for Phase 4 & 5)
	, 'S5SMOKE'
	, 'S5KD'
		)
) a on a.target_concept_id = o.observation_concept_id
group by observation_concept_id, left( observation_source_value , CHARINDEX('=', observation_source_value, 1) -2)
order by 2




-- Unpivot the source table. 
if object_id('tempdb.dbo.#obs') is not null drop table #obs
SELECT IDNO, obs_name, obs_value, 
	cast(NULL as datetime) as obs_date,  
	cast(NULL as varchar(10)) as Phase,
	cast(NULL as varchar(150)) as value_as_string
into #obs
FROM   
   (SELECT [IDNO]
	, S5ESTRO
	, S5ETOH
	, S5ACR
	--, S5ACR2
	, S5SMOKE
	, S5KD

   FROM [omoprawdata].dbo.S5ALL23 ) p  
UNPIVOT  
   (obs_value FOR obs_name IN   
      (	  
		S5ESTRO
		, S5ETOH
		, S5ACR
		--, S5ACR2
		, S5SMOKE
		, S5KD

	  )  
)AS unpvt;  
GO


select * from #obs 

/***********************************************************
need to map the values in source DB to acutal string values 
***********************************************************/
--add the Observation date and units 
update mg set obs_date =   rw.S5EXDATE 
, Phase = 'Phase V' 
, value_as_string = case 
		when obs_name like '__ESTRO' and obs_value = 'CUR' Then 'CUR: MEDICATION HISTORY INDICATED ESTROGEN USE'
		when obs_name like '__ESTRO' and obs_value = 'EX-' Then 'EX-: IF USED ESTROGEN AND NOT LIST ESTROGEN ON MEDICAL HISTORY'
		when obs_name like '__ESTRO' and obs_value = 'NEV' Then 'NEV: IF NEVER USED ESTROGEN'

		when obs_name like '__ETOH' and obs_value = 'N' Then 'N: HAVE NOT HAD MORE THAN 12 DRINKS IN ENTIRE LIFE'
		when obs_name like '__ETOH' and obs_value = 'E' Then 'E: HAD MORE THAN 12 DRINKS IN ENTIRE LIFE AND HAS NOT HAD A DRINK IN 12 OR MORE MONTHS (MORE THAN 1 YEAR)'
		when obs_name like '__ETOH' and obs_value = 'Y' Then 'Y: HAD MORE THAN 12 DRINKS IN ENTIRE LIFE AND HAS HAD A DRINK IN THE LAST 12  MONTHS (1 YEAR)'

		when obs_name like '__ACR' and obs_value = '1' Then 'NORMAL: URINARY ALBUMIN/CREATININE RATIO ≥ 300 mg/g'
		when obs_name like '__ACR' and obs_value = '2' Then 'MICROALBUMINURIA: 30 ≤ URINARY ALBUMIN/CREATININE RATIO < 299 mg/g'
		when obs_name like '__ACR' and obs_value = '3' Then 'MACROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO < 30 mg/g'

		--when obs_name like '__ACR2' and obs_value = 'Y' Then ' MICROALBUMINURIA (30 ≤ URINARY ALBUMIN/CREATININE RATIO < 299 mg/g) AND MACROALBUMINURIA (URINARY ALBUMIN/CREATININE RATIO < 30 mg/g)'
		--when obs_name like '__ACR2' and obs_value = 'N' Then 'NORMAL (URINARY ALBUMIN/CREATININE RATIO ≥ 300 mg/g)'

		when obs_name like '__SMOKE' and obs_value = 'N' Then 'N: HAVE NOT SMOKED MORE THAN 100 CIGARETTES IN ENTIRE LIFE OR NEVER SMOKED REGULARLY'
		when obs_name like '__SMOKE' and obs_value = 'E' Then 'E: SMOKED AT LEAST 100 CIGARETTES IN ENTIRE LIFE AND ENTERED AGE STARTED SMOKING AND DO NOT SMOKE CURRENTLY'
		when obs_name like '__SMOKE' and obs_value = 'Y' Then 'Y: SMOKED AT LEAST 100 CIGARETTES IN ENTIRE LIFE AND ENTERED AGE STARTED SMOKING AND CURRENTLY SMOKED'

		else obs_value

	end
 from #obs mg
join [omoprawdata].dbo.S5ALL23 rw on rw.idno = mg.idno 


select * from #obs
select distinct obs_name,obs_value,value_as_string from #obs order by obs_name, obs_value

-- Phase V
insert into  omop1.[dbo].[observation](
	   [observation_id]
      ,[person_id]
      ,[observation_concept_id]
      ,[observation_date]
      ,[observation_datetime]
      ,[observation_type_concept_id]
	  ,[value_as_string]
      ,[value_as_concept_id]
      ,[unit_concept_id]
      ,[provider_id]
      ,[visit_occurrence_id]
      ,[observation_source_value]
      ,[observation_source_concept_id]
      ,[unit_source_value]
      ,[qualifier_source_value]
	  , obs_event_field_concept_id
	  )
select next Value for  dbo.observation_seq as [observation_id]
      , a.[person_id]					as person_id
      ,	a.target_concept_id 			as [observation_concept_id]
      ,	a.observation_date				as [observation_date]
      , a.observation_date				as [observation_datetime]
      , 44814721						as [observation_type_concept_id]  --		Patient reported
	  , a.value_as_string				as [value_as_string]
      , a.value_as_concept_id			as [value_as_concept_id]
      , 0								as [unit_concept_id] 
      , 1								as [provider_id]
      , a.visit_occurrence_id			as [visit_occurrence_id]
      , a.observation_source_value		as [observation_source_value]
      , 0								as [observation_source_concept_id]
      , NULL							as [unit_source_value]
      , NULL							as [qualifier_source_value]
	  , 0								as [obs_event_field_concept_id]

from (
	

	select pat.[IDNO]						as person_id
	, pat.obs_date							as observation_date
	, obs_name + ' = '
		+ obs_value							as observation_source_value
	, value_as_string						as [value_as_string]
	, 0										as [value_as_concept_id]
	, vo1.visit_occurrence_id				as visit_occurrence_id
	, shs1.target_concept_id				as target_concept_id 
	, 'Phase V'								as phase
	from #obs pat
	left join omop1.dbo.visit_occurrence vo1 on vo1.person_id = pat.[IDNO] 
		and vo1.visit_source_value like 'SHS Phase V %'
	left join  omop1.dbo.source_to_concept_map shs1 on shs1.source_code= pat.obs_name
	where pat.obs_name like 'S5%'


)a

