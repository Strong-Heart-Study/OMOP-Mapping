use omop1
go


/*********************************************************************
	Numeric values
********************************************************************/

--destination tables 
-----------------------------------
--#records per phase/ concept in measurement and observation (Numeric)
if object_id('tempdb.dbo.#destination') is not null drop table #destination
select a.*, c.domain_id, c.standard_concept, c.invalid_reason
into #destination
from (
	select trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 )) Phase
	,  m.measurement_concept_id OMOP_Concept_ID
	, min(m.value_as_number) min_value, max(m.value_as_number) max_value
	, avg(m.value_as_number) mean_value, cast(0.0 as float) as median_value
	, count(*) counts 
	from omop1.dbo.measurement m
	left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = m.visit_occurrence_id
	where m.value_as_number is not null 
	group by m.measurement_concept_id, trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 ))
	union 
	select trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 )) Phase
	, m.observation_concept_id OMOP_Concept_ID	
	, min(m.value_as_number) min_value, max(m.value_as_number) max_value
	, avg(m.value_as_number) mean_value, cast(0.0 as float) as median_value
	, count(*) counts 
	from omop1.dbo.observation m
	left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = m.visit_occurrence_id
	where ISNUMERIC( m.value_as_number) =1	
	group by m.observation_concept_id, trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 ))
	) a
left join omop1.dbo.concept c on c.concept_id = a.OMOP_Concept_ID


if object_id('tempdb.dbo.#median') is not null drop table #median
select measurement_concept_id OMOP_Concept_ID, measurement_source_value
, PERCENTILE_CONT(0.5) within group (order by m.value_as_number) over(partition by m.measurement_concept_id, trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 ))) median_value 
into #median
from omop1.dbo.measurement m 
left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = m.visit_occurrence_id
where ISNUMERIC( m.value_as_number) =1
union 
select observation_concept_id OMOP_Concept_ID, observation_source_value 
, PERCENTILE_CONT(0.5) within group (order by m.value_as_number) over(partition by m.observation_concept_id, trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 ))) median_value 
from omop1.dbo.observation m 
left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = m.visit_occurrence_id
where ISNUMERIC( m.value_as_number) =1


update d
set d.median_value  = med.median_value
from #destination d
left join #median med on med.OMOP_Concept_ID = d.OMOP_Concept_ID



select * from #destination




--source tables
-----------------------------------
begin

--Phase 123
	--Numeric values 
	if object_id('tempdb.dbo.#numeric_phase123') is not null drop table #numeric_phase123
	SELECT idno, variables, meas_value, 'Numeric' Variable_Type
	--into #numeric_phase123
	FROM
	(
	  select * from omoprawdata.dbo.SHSALL33
	) AS cp
	UNPIVOT 
	(
		  meas_value FOR variables IN ( 
			  S1TG, S1TC, S1HDL, S1G0, S1WHR, S1UACR, S1LACR, S1INSU
			, S1FIBRIN, S1APOB, S1APOAI, S1BDFAT, S1G2, S1SBP, S1DBP, S1BMI, S1P_CREA, S1HBA1C, S1HT, S1WT, S1HIP
			, S1WAIST, S1U_CREA, S1RT_AAR, S1LT_AAR, BLOODALL, S2U_CREA, S2SBP, S2DBP, S2HT, S2WT, S2BMI, S2BDFAT
			, S2WHR, S2TC, S2HDL, S2TG, S2INSU, S2P_CREA, S2UACR, S2LACR, S2HBA1C, S2RT_AAR, S2LT_AAR, S2G2, S2G0
			, S2FIBRIN, S2HIP, S2WAIST, S3BDFAT, S3BMI, S3DBP, S3SBP, S3WT, S3HT, S3WHR, S3RT_AAR, S3LT_AAR, S3G0
			, S3G2, S3TG, S3TC, S3HDL, S3U_CREA, S3P_CREA, S3INSU, S3HBA1C, S3FIBRIN, S3UACR, S3LACR, S3HIP
			, S3WAIST, S1LDLBQ, S1LDLEST, S2LDLBQ, S2LDLEST, S3LDLEST, S1ADADMD, S2ADADMD, S3ADADMD, S1WHODMD
			, S2WHODMD, S3WHODMD, S1WHNDMD, S2WHNDMD, S3WHNDMD, S1AGE, S2AGE, S3AGE, S1SMKD, S1PPY, S2PPY
			, S2SMKD, S3PPY, S3SMKD, S1GFR, S1CCR, S1MBP, S1PP, S1HOMAIR, S2GFR, S2CCR, S2MBP, S2PP, S2HOMAIR
			, S3GFR, S3CCR, S3MBP, S3PP, S3HOMAIR, S1MSCORE, S2MSCORE, S3MSCORE, S1U_ALB, MED20, S2U_ALB, S2CRP
			, S2PAI1, MED2_2, MED2_7, S3U_ALB, MED3_2, MED3_10, S1BSA, S1PCREAC, S1GFRM2, S1GFR_MC, S1GFR_EPI
			, S1GFR_EPIC, S2GFRM2, S2GFR_EPI, S3GFRM2, S3GFR_EPI, S1CKD, S2CKD, S2CKDSTAGE, S3CKD, S3CKDSTAGE
		)
	) AS up;


	select * from #numeric_phase123

	if object_id('tempdb.dbo.#median123') is not null drop table #median123
	select variables, Variable_Type
	, PERCENTILE_CONT(0.5) within group (order by m.meas_value) over(partition by variables) median_value 
	into #median123
	from #numeric_phase123 m 

	--combine numeric and text phase123 variables
	if object_id('tempdb.dbo.#phase123') is not null drop table #phase123
	select p.variables, p.variable_type, count(distinct p.idno) as counts
	, min(p.meas_value) as min_value
	, max(p.meas_value) as max_value
	, avg(p.meas_value) as mean_value
	, cast(0.0 as float) as median_value 
	into #phase123 
	from #numeric_phase123 p
	group by p.variables, p.variable_type
	--union 
	--select  variables, variable_type, count(distinct idno) as counts  
	--, 0.0 as min_value, 0.0 as max_value, 0.0 as mean_value, cast(0.0 as float) as median_value 
	--from #text_phase123
	--group by variables, variable_type



	update p 
	set p.median_value = m.median_value
	from #phase123 p 
	left join #median123 m on m.variables = p.variables
	where p.Variable_Type = 'Numeric'

	select * from #phase123

	

end 



--Phase 4
begin

	--Numeric values 
	if object_id('tempdb.dbo.#numeric_phase4') is not null drop table #numeric_phase4
	SELECT idno, variables, meas_value, 'Numeric' Variable_Type
	--into #numeric_phase4
	FROM
	(
	  select * from omoprawdata.dbo.S4ALL23
	) AS cp
	UNPIVOT 
	(
		  meas_value FOR variables IN ( 
			S4AGE, S4EDU, S4BLOOD, S4SMKD, S4PPY, EX4_42, S4HT, S4WT, S4HIP, S4WAIST
			, S4BMI, S4WHR, S4LBM, S4FAT, S4BDFAT, S4SBP, S4DBP, S4MBP, S4PP, S4RT_AAR, S4LT_AAR, S4WBC
			, S4RBC, S4HGB, S4HCT, S4MCV, S4PLT, S4NEUT, S4PAI1, S4U_ALB, S4P_CREA, S4UACID, S4INSU, S4G0
			, S4FIBRIN, S4TC, S4HDL, S4TG, S4LDL, S4ALT, S4AST, S4ALB, S4BUN, S4CO2, S4CAL, S4CHL, S4PHOSP
			, S4POT, S4SOD, S4TBILI, S4TPROT, S4HBA1C, S4APOA1, S4APOB, S4U_CREA, S4GFR, S4CCR, S4UACR
			, S4LACR, S4HOMAIR, S4MSCORE, S4MSCORN, S4CRP, s4dmn1, MED4_10, S4ADADMD, S4GFRM2, S4GFR_EPI
		)
	) AS up;



	
	
	if object_id('tempdb.dbo.#median4') is not null drop table #median4
	select variables, Variable_Type
	, PERCENTILE_CONT(0.5) within group (order by m.meas_value) over(partition by variables) median_value 
	into #median4
	from #numeric_phase4 m 


	--combine numeric and text phase 4 variables
	if object_id('tempdb.dbo.#phase4') is not null drop table #phase4
	select variables, variable_type, count(distinct idno) as counts 
	, min(p.meas_value) as min_value
	, max(p.meas_value) as max_value
	, avg(p.meas_value) as mean_value
	, cast(0.0 as float) as median_value 
	into #phase4 
	from #numeric_phase4 p
	group by p.variables, p.variable_type
	--union 
	--select  variables, variable_type, count(distinct idno) as counts  
	--, 0.0 as min_value, 0.0 as max_value, 0.0 as mean_value, cast(0.0 as float) as median_value 
	--from #text_phase4 p
	--group by p.variables, p.variable_type


	
	update p 
	set p.median_value = m.median_value
	from #phase4 p 
	left join #median4 m on m.variables = p.variables
	where p.Variable_Type = 'Numeric'

	select * from #phase4


end 



--Phase 5
begin

	--Numeric values 
	if object_id('tempdb.dbo.#numeric_phase5') is not null drop table #numeric_phase5
	SELECT idno, variables, meas_value, 'Numeric' Variable_Type
	--into #numeric_phase5
	FROM
	(
	  select * from omoprawdata.dbo.S5ALL23
	) AS cp
	UNPIVOT 
	(
		  meas_value FOR variables IN ( 
			 EX5_42, S5HT, S5WT, S5HIP, S5WAIST, S5BMI
			, S5WHR, S5LBM, S5FAT, S5BDFAT, S5SBP, S5DBP, S5MBP, S5PP, S5RT_AAR
			, S5LT_AAR, S5WBC, S5RBC, S5HGB, S5HCT, S5MCV, S5PLT, S5NEUT, S5P_CREA
			, S5CRP, S5HDL, S5G0, S5INSU, S5LDL, S5TG, S5U_ALB, S5TC, S5HBA1C
			, S5UCREA, S5CCR, S5UACR, S5LACR, S5HOMAIR, S5ADADMD, S5UACID, S5EDU, S5AGE
			, S5GFRM2, S5GFR_EPI, S5PPY, s5smkd, S5P_ALB, S5CKD, S5CKDSTAGE
		)
	) AS up;



	
	if object_id('tempdb.dbo.#median5') is not null drop table #median5
	select variables, Variable_Type
	, PERCENTILE_CONT(0.5) within group (order by m.meas_value) over(partition by variables) median_value 
	into #median5
	from #numeric_phase5 m 

	--combine numeric and text phase 5 variables
	if object_id('tempdb.dbo.#phase5') is not null drop table #phase5
	select variables, variable_type, count(distinct idno) as counts 
	, min(p.meas_value) as min_value
	, max(p.meas_value) as max_value
	, avg(p.meas_value) as mean_value
	, cast(0.0 as float) as median_value 
	into #phase5 
	from #numeric_phase5 p
	group by p.variables, p.variable_type
	--union 
	--select  variables, variable_type, count(distinct idno) as counts  
	--, 0.0 as min_value, 0.0 as max_value, 0.0 as mean_value, cast(0.0 as float) as median_value 
	--from #text_phase5 p
	--group by p.variables, p.variable_type

		
	update p 
	set p.median_value = m.median_value
	from #phase5 p 
	left join #median5 m on m.variables = p.variables
	where p.Variable_Type = 'Numeric'

	select * from #phase5

end 



--Assign phase and concept iD for all phases (Numeric)
if object_id('tempdb.dbo.#Source_counts') is not null drop table #Source_counts
select sc.target_concept_id OMOP_Concept_ID 
	, case
		when nc.variables like '_1%' then 'Phase I'
		when nc.variables like '_2%' then 'Phase II'
		when nc.variables like '_3%' then 'Phase III'
		when nc.variables like '_4%' then 'Phase IV'
		when nc.variables like '_5%' then 'Phase V'
	end Phase
	,  nc.variables, nc.Variable_Type,  nc.counts 
	, nc.min_value, nc.max_value, nc.mean_value, nc.median_value
into #Source_counts
from
(
	select * from #phase123
	union 
	select * from #phase4
	union
	select * from #phase5
 ) nc
left join omop1.dbo.source_to_concept_map sc on sc.source_code = nc.variables

select * from #Source_counts





/*********************************************************************
-- compare source and destination tables
********************************************************************/


--Numeric
select sc.Phase [Source_Phase], sc.variables [Source_variable], sc.variable_type, sc.counts [Source_Counts]
, sc.min_value, sc.max_value, sc.mean_value, sc.median_value
, d.Phase [Dest_Phase], d.OMOP_Concept_ID [Dest_OMOP_Concept_id], d.counts [Dest_counts]
, d.min_value, d.max_value, d.mean_value, d.median_value
, d.domain_id, d.standard_concept, d.invalid_reason
, case when sc.counts = d.counts and sc.counts is not null and d.counts is not null 
	 then 1 else 0 end as [Counts_matched]
, case when sc.min_value = d.min_value and sc.min_value is not null and d.min_value is not null 
	 then 1 else 0 end as [Min_value_matched]
, case when sc.max_value = d.max_value and sc.max_value is not null and d.max_value is not null 
	 then 1 else 0 end as [max_value_matched]
, case when cast(round(sc.mean_value, 2) as numeric(10,2)) = cast(round(d.mean_value,2) as numeric(10,2)) and sc.mean_value is not null and d.mean_value is not null 
	 then 1 else 0 end as [mean_matched]
, case when sc.median_value = d.median_value and sc.median_value is not null and d.median_value is not null 
	 then 1 else 0 end as [Median_matched]
from #Source_counts sc
left join #destination d on sc.OMOP_Concept_ID = d.OMOP_Concept_ID 
	and sc.Phase = d.Phase
where sc.Variable_Type = 'Numeric'



select * from #destination 





/*********************************************************************
	Text variables (needs cleanup)

********************************************************************/



-- Destination variables 
if object_id('tempdb.dbo.#destination_text') is not null drop table #destination_text
select a.*, c.domain_id, c.standard_concept, c.invalid_reason
into #destination_text
from (
	select trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 )) Phase
	,  m.measurement_concept_id OMOP_Concept_ID
	, m.value_source_value
	, count(*) counts 
	from omop1.dbo.measurement m
	left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = m.visit_occurrence_id
	where m.value_as_number is  null 
	group by m.measurement_concept_id, trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 ))
	, m.value_source_value
	union 
	select trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 )) Phase
	, m.observation_concept_id OMOP_Concept_ID	
	, m.value_as_string
	, count(*) counts 
	from omop1.dbo.observation m
	left join omop1.dbo.visit_occurrence vo on vo.visit_occurrence_id = m.visit_occurrence_id
	where m.value_as_number is  null 	
	group by m.observation_concept_id, trim(SUBSTRING(vo.visit_source_value, 5, CHARINDEX(' ', vo.visit_source_value, 11) -5 ))
	, m.value_as_string
	) a
left join omop1.dbo.concept c on c.concept_id = a.OMOP_Concept_ID



--- Source tables

	--text values
	if object_id('tempdb.dbo.#text_phase123') is not null drop table #text_phase123
	SELECT idno, variables, meas_value, 'Text' Variable_Type
	into #text_phase123
	FROM
	(
		select * from omoprawdata.dbo.SHSALL33
	) AS cp
	UNPIVOT 
	(
		  meas_value FOR variables IN ( S1ADADM, S2ADADM, S3ADADM, S1ACR, S1ETOH, S1SMOKE, S1DMTX, S1EDU
		, S1INCOME, S1HTNRX2, S1USHTN2, S1WHOHN2, TRIBE, S2INCOME, S2SMOKE, S2DMTX, S2ETOH
		, S2ACR, S2HTNRX3, S2WHOHN2, S2USHTN2, S3USHTN, S3WHOHTN, S3HTNRX, S3SMOKE, S3INCOME
		, S3ACR, S1MENO, S1ESTRO, S1WHODM, S2WHODM, S3WHODM, S1WHNDM, S2WHNDM, S3WHNDM, S2MENO
		, S2ESTRO, S3MENO, S3ESTRO, S1ATP3MS, S2ATP3MS, S3ATP3MS, S1HTNHX, S1DMHX, S1DIALYS
		, S1KIDTRA, S2HTNHX, S2DMHX, S2DIALYS, S2KIDTRA, S3HTNHX, S3DMHX, S3DIALYS, S3KIDTRA
		, S3ETOH2, S3DMTX2, S1LDRINK, S1MONAGO, MED27, S2LDRINK, S2MONAGO, S3LDRINK, S3MONAGO
		, S1ATP3MS0, S2ATP3MS0, S3ATP3MS0, S1HYLIPID, S2HYLIPID, S3HYLIPID, S1CKDSTAGE
		)
	) AS up;
	


	--text values
	if object_id('tempdb.dbo.#text_phase4') is not null drop table #text_phase4
	SELECT idno, variables, meas_value, 'Text' Variable_Type
	into #text_phase4
	FROM
	(
		select * from omoprawdata.dbo.S4ALL23
	) AS cp
	UNPIVOT 
	(
		  meas_value FOR variables IN ( 
			 FAMID, S4INCOME, S4KF, S4KD, S4KT, INT24_67, S4ETOH
			, S4SMOKE, S4HTNRX, S4ACR, S4ACR2, MED4_8, S4DMTX, S4MENO, S4ESTRO, S4LPTRT, S4HTNHX
			, S4DMHX, S4ATP3MS, S4USHTN, S4HTN2, S4HTNHX2, S4HTNRX2, S4ATP3MSN, S4ADADM, S4ATP3MS0
		)
	) AS up;



	--text values
	if object_id('tempdb.dbo.#text_phase5') is not null drop table #text_phase5
	SELECT idno, variables, meas_value, 'Text' Variable_Type
	into #text_phase5
	FROM
	(
		select * from omoprawdata.dbo.S5ALL23
	) AS cp
	UNPIVOT 
	(
		  meas_value FOR variables IN ( 
			 FAMID, S5INCOME, S5KF, S5KD, S5KT, INT25_67, S5ETOH, S5SMOKE, S5HTNRX
			, S5WHOHTN, S5USHTN, S5HTNHX, S5ACR, S5ACR2, S5LPTRT, S5DMHX, S5DMTX
			, S5ADADM, S5ATP3MS0, s5meno, s5estro, S5bingeM, S5bingeY, S5HYLIPID
		)
	) AS up;
	

	
--Assign phase and concept iD for all phases (Text)
if object_id('tempdb.dbo.#text_source_counts') is not null drop table #text_source_counts
select sc.target_concept_id OMOP_Concept_ID 
	, case
		when nc.variables like '_1%' then 'Phase I'
		when nc.variables like '_2%' then 'Phase II'
		when nc.variables like '_3%' then 'Phase III'
		when nc.variables like '_4%' then 'Phase IV'
		when nc.variables like '_5%' then 'Phase V'
	end Phase
	,  nc.variables, nc.Variable_Type,  nc.meas_value 
	,  count(*) counts
into #text_source_counts
from
(
	select * from #text_phase123
	union 
	select * from #text_phase4
	union
	select * from #text_phase5
 ) nc
left join omop1.dbo.source_to_concept_map sc on sc.source_code = nc.variables
group by sc.target_concept_id, nc.variables, nc.Variable_Type, nc.meas_value

select * from #text_source_counts


--text 
select sc.Phase [Source_Phase], sc.variables [Source_variable], sc.variable_type, sc.meas_value [Source_value], sc.counts [Source_Counts]
, d.Phase [Dest_Phase], d.OMOP_Concept_ID [Dest_OMOP_Concept_id], d.counts [Dest_counts]

from #text_source_counts sc 
left join #destination d on sc.OMOP_Concept_ID = d.OMOP_Concept_ID
	and sc.Phase = d.Phase
where sc.Variable_Type = 'Text'


