
use omoprawdata 
go

-- Add mapping to source_to_concept_map table
begin 
	insert into omop1.dbo.source_to_concept_map ( source_code, source_concept_id, source_code_description, source_vocabulary_id, target_concept_id, target_vocabulary_id
		, valid_start_date, valid_end_date)

	--4087498	Total body fat	Measurement	SNOMED	Observable Entity	S	248361005	2002-01-31	2099-12-31	NULL
	select 'S5FAT', 0, 'SHS5 BODY FAT (kg)', 'SHS', 4087498, 'SNOMED', '1996-09-08',	'2099-12-31' -- condition mapping (standard concept)
	

END 

----------------------------------------------------------------

/* Update concept_id of S5FAT to 4087498 (total body fat) from 3032843 (% body fat)  */

--update  meas set measurement_concept_id = 4087498	-- total body fat
select *
from omop1.dbo.measurement meas
where measurement_concept_id = 3032843 -- % body fat
and unit_concept_id = 9529 --kg
and measurement_source_value like '%s5fat%'




---------------------------
