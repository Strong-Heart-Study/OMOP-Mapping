-- List of concept ids used in StrongHeart Study
use omop1

select 'condition' table_name, condition_concept_id domain_concept_id
, NULL as value_as_concept_id, NULL value_source_value
, NULL unit_concept_id, NULL unit_source_value,  count(*) counts
from dbo.condition_occurrence co 
group by condition_concept_id

union

select  'drug_exposure' table_name, drug_concept_id
, NULL as value_as_concept_id, NULL value_source_value
, NULL unit_concept_id, NULL unit_source_value, count(*) counts
from dbo.drug_exposure de
group by drug_concept_id 

union

select  'measurement' table_name, measurement_concept_id
, value_as_concept_id, value_source_value
, unit_concept_id, unit_source_value, count(*) counts
from dbo.measurement de
group by measurement_concept_id , value_as_concept_id, value_source_value
, unit_concept_id, unit_source_value

union

select  'observation' table_name, observation_concept_id
, value_as_concept_id, observation_source_value
, unit_concept_id, unit_source_value, count(*) counts
from dbo.observation de
group by observation_concept_id , value_as_concept_id, observation_source_value
, unit_concept_id, unit_source_value


