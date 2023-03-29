/*
	Summary statistics for measurement values 
*/

use OMOP1
go

if object_id('tempdb.dbo.#measurments') is not null drop table #measurments 
select m.measurement_concept_id, trim(SUBSTRING(v.visit_source_value, 11, CHARINDEX(' ', v.visit_source_value, 11) -11 )) Phase
, sc.source_code, m.measurement_source_value
	, avg(m.value_as_number) avg_value, min(m.value_as_number) min_value, max(m.value_as_number) max_value 
	, count(distinct m.person_id) counts
	, m.unit_source_value
	into #measurments
from omop1.dbo.measurement m 
join omop1.dbo.visit_occurrence v on v.visit_occurrence_id = m.visit_occurrence_id
left join omop1.dbo.source_to_concept_map sc on sc.target_concept_id = m.measurement_concept_id 
	and SUBSTRING(sc.source_code, 2,1) = case trim(SUBSTRING(v.visit_source_value, 11, CHARINDEX(' ', v.visit_source_value, 11) -11 ))
												when 'I' then 1 
												when 'II' then 2
												when 'III' then 3 
												when 'IV' then 4
												when 'V' then 5 
												else 0
												end
group by m.measurement_concept_id, trim(SUBSTRING(v.visit_source_value, 11, CHARINDEX(' ', v.visit_source_value, 11) -11 ))
	, sc.source_code, m.measurement_source_value
	, m.unit_source_value



select *
from #measurments m 
	

----- Source 

	--Numeric values (Phase 123)
	if object_id('tempdb.dbo.#numeric_phase123') is not null drop table #numeric_phase123
	SELECT idno, variables, meas_value
	into #numeric_phase123
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

	--Numeric values  (Phase 4)
	if object_id('tempdb.dbo.#numeric_phase4') is not null drop table #numeric_phase4
	SELECT idno, variables, meas_value
	into #numeric_phase4
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

	--Numeric values (Phase 5) 
	if object_id('tempdb.dbo.#numeric_phase5') is not null drop table #numeric_phase5
	SELECT idno, variables, meas_value
	into #numeric_phase5
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

	select * from #numeric_phase5


	---combine all varaiables
	if object_id('tempdb.dbo.#source_consolidated') is not null drop table #source_consolidated
	select idno, variables
		, case left(variables,1) when 'S' then right(variables, len(variables)-2) else variables end variable_common
		, meas_value 
	into #source_consolidated
	from #numeric_phase123
	union all 
	select idno, variables
		, case left(variables,1) when 'S' then right(variables, len(variables)-2) else variables end variable_common
		, meas_value 
	from #numeric_phase4
	union all 
	select idno, variables
		, case left(variables,1) when 'S' then right(variables, len(variables)-2) else variables end variable_common
		, meas_value 
	from #numeric_phase5			
	
	
	--source variables statistics
	select variable_common, avg(meas_value) avg_value, min(meas_value) min_value, max(meas_value) max_value
	, count(distinct idno) counts 
	from #source_consolidated		
	group by variable_common		



--compare source and destination summary statistics
select sc.*
, m.*
from #source_consolidated sc 
left join #measurments m on sc.variables = m.source_code



