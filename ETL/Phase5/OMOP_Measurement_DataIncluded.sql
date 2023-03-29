select  measurement_concept_id, measurement_source_value, count(*) counts 
from omop1.dbo.measurement 
where measurement_source_value like 's5%'
group  by measurement_concept_id, measurement_source_value