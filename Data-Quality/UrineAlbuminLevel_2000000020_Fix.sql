/*
        Kai Post
        September 21, 2022
        The entries in Oklahoma OMOP for the categoric observation "Urine Albumin Level" (37392374) are confusing.
        
        Note: On September 21, 2022 we created a new concept_id for Albuminuria Status, 2000000020. We also changed the
        concept_id for the albumine/creatinine ratio from 3034485 to 3001802.
        
        
        Old Categoric Choices:
        
        NORMAL: URINARY ALBUMIN/CREATININE RATIO = 300 mg/g
        MICROALBUMINURIA: 30 = URINARY ALBUMIN/CREATININE RATIO < 299 mg/g
        MACROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO < 30 mg/g
        
        
        
        Categoric Choices Updated September 21, 2022 to fall in line with the definition of the National Kidney Foundation:
        
        NORMAL: URINARY ALBUMIN/CREATININE RATIO < 30 mg/g
        MICROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO >= 30 mg/g and <= 300 mg/g
        MACROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO > 300 mg/g
        
        https://www.kidney.org/kidneydisease/siemens_hcp_acr
        
 
        
        Related Existing Oklahoma Measurements from which the Albuminuria Status is derived:
        
        Albumin/Creatinine [Mass Ratio] in Urine*        3001802         8723    mg/g
 */

// To compare the measurements to the observations version, get counts from measurement:
select distinct level_derived, count(*) as count_derived
from(
        select 
        case
                when measurement.value_as_number < 30 then 'NORMAL'
                when measurement.value_as_number >= 30 and measurement.value_as_number <= 300 then 'MICRO'
                when measurement.value_as_number > 300 then 'MACRO'
                else 'INVALID'
        end as level_derived
        from omop1.dbo.measurement
        where measurement.measurement_concept_id = 3001802
) a
group by level_derived;


// To compare the measurements to the observations version, get counts from observation:
select distinct level_stored, count(*) as count_stored
from(
        select 
        case
                when observation.value_as_string = 'NORMAL: URINARY ALBUMIN/CREATININE RATIO < 30 mg/g' then 'NORMAL'
                when observation.value_as_string = 'MICROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO >= 30 mg/g and <= 300 mg/g' then 'MICRO'
                when observation.value_as_string = 'MACROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO > 300 mg/g' then 'MACRO'
                else 'INVALID'
        end as level_stored
        from omop1.dbo.observation
        where observation.observation_concept_id = 2000000020
) a
group by level_stored;


// Compare them side-by side:
select *
from(
        SELECT observation.person_id ,
        case
                when observation.value_as_string = 'NORMAL: URINARY ALBUMIN/CREATININE RATIO < 30 mg/g' then 'NORMAL'
                when observation.value_as_string = 'MICROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO >= 30 mg/g and <= 300 mg/g' then 'MICROALBUMINURIA'
                when observation.value_as_string = 'MACROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO > 300 mg/g' then 'MACROALBUMINURIA'
                else 'INVALID'
        end as categoric_value ,
        measurement.value_as_number as UACR,
        observation_source_value,
        CASE
                when measurement.value_as_number < 30 THEN 'NORMAL'  -- Normal
                when measurement.value_as_number >= 30 and cast(measurement.value_as_number as float) <= 300 then 'MICROALBUMINURIA' -- Micro
                when measurement.value_as_number > 300 then 'MACROALBUMINURIA' -- Macro
                else 'INVALID'
        end as calculated_value
        FROM omop1.DBO.observation
        join omop1.dbo.measurement on observation.person_id= measurement.person_id and observation.visit_occurrence_id = measurement.visit_occurrence_id
        where measurement.measurement_concept_id = 3001802
        and observation.observation_concept_id = 2000000020
) a
where categoric_value != calculated_value;


// Create a temproary table from measurement that contains the values to update the observation table:
select 
        person_id,
        visit_occurrence_id,
        measurement.value_as_number,        
        case
                when measurement.value_as_number < 30 then 'NORMAL: URINARY ALBUMIN/CREATININE RATIO < 30 mg/g'
                when measurement.value_as_number >= 30 and measurement.value_as_number <= 300 then 'MICROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO >= 30 mg/g and <= 300 mg/g'
                when measurement.value_as_number > 300 then 'MACROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO > 300 mg/g'
                else 'INVALID' -- This possibility should have been addressed by now.
        end as value_as_string
into #albuminuria
from omop1.dbo.measurement
where measurement.measurement_concept_id = 3001802;


// Testing:
select *
from #albuminuria;


// Create a temporarary backup table for observation, just in case:
select *
into #observation_backup
from omop1.DBO.observation;


// Testing:
select count(*) from omop1.DBO.observation;
select count(*) from #observation_backup;


// Does this update look right?
update o
set o.value_as_string = a.value_as_string
from omop1.DBO.observation o
inner join #albuminuria a on o.person_id= a.person_id and o.visit_occurrence_id = a.visit_occurrence_id
where o.observation_concept_id = 2000000020;

// Outcome: In one single instance observation and measurement did not match. In that case the measurement value_as_number was exactly
// 300. We changed that observation from macro to micro.
update omop1.dbo.observation
set value_as_string = 'MICROALBUMINURIA: URINARY ALBUMIN/CREATININE RATIO >= 30 mg/g and <= 300 mg/g'
where observation_id = 264829
   
