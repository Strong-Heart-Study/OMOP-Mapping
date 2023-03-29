#!/usr/bin python3

sql_gender = "select distinct 'Demographics' as 'domain', 'Gender' as 'concept_name', null as 'concept_id', (select count(*) from person where gender_concept_id not in(8507, 8532)) as 'persons_missing', (select count(*) from {}) as 'persons_total' from {}"
sql_birth = "select distinct 'Demographics' as 'domain', 'Date of Birth' as 'concept_name', null as 'concept_id', (select count(*) from person where birth_datetime is null) as 'persons_missing', (select count(*) from {}) as 'persons_total' from {}"
sql_death = "select distinct 'Demographics' as 'domain', 'Date of Death' as 'concept_name', null as 'concept_id', (select count(*) from person where death_datetime is null) as 'persons_missing', (select count(*) from {}) as 'persons_total' from {}"
sql = "select '{}' as 'domain', (select concept_name from {} where concept_id = {}) as 'concept_name', {} as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from {}) as 'persons_total' from {} where person_id not in (select distinct person_id from {} where {} = {})"
union = "union all"

#person_table = 'person'
#concept_table = 'concept'
#condition_table = 'condition_occurrence'
#measurement_table = 'measurement'
#observation_table = 'observation'

person_table = 'omop1.dbo.person'
concept_table = 'omop1.dbo.concept'
condition_table = 'omop1.dbo.condition_occurrence'
drug_table = 'omop1.dbo.drug_exposure'
measurement_table = 'omop1.dbo.measurement'
observation_table = 'omop1.dbo.observation'

condition_concept_ids_file = 'ok_condition_concept_ids.txt'
drug_concept_ids_file = 'ok_drug_concept_ids.txt'
measurement_concept_ids_file = 'ok_measurement_concept_ids.txt'
observation_concept_ids_file = 'ok_observation_concept_ids.txt'

print(sql_gender.format(person_table, person_table))
print(union)
print(sql_birth.format(person_table, person_table))
print(union)
print(sql_death.format(person_table, person_table))

file_c = open(condition_concept_ids_file, 'r')
condition_concept_ids = file_c.readlines()
for condition_concept_id in condition_concept_ids:
    print(union)
    print(sql.format('Condition', concept_table, condition_concept_id.strip(), condition_concept_id.strip(), person_table, person_table, condition_table, 'condition_concept_id', condition_concept_id.strip()))
    
file_d = open(drug_concept_ids_file, 'r')
drug_concept_ids = file_d.readlines()
for drug_concept_id in drug_concept_ids:
    print(union)
    print(sql.format('Drug', concept_table, drug_concept_id.strip(), drug_concept_id.strip(), person_table, person_table, drug_table, 'drug_concept_id', drug_concept_id.strip()))

file_m = open(measurement_concept_ids_file, 'r')
measurement_concept_ids = file_m.readlines()
for measurement_concept_id in measurement_concept_ids:
    print(union)
    print(sql.format('Measurement', concept_table, measurement_concept_id.strip(), measurement_concept_id.strip(), person_table, person_table, measurement_table, 'measurement_concept_id', measurement_concept_id.strip()))

file_o = open(observation_concept_ids_file, 'r')
observation_concept_ids = file_o.readlines()
for observation_concept_id in observation_concept_ids:
    print(union)
    print(sql.format('Observation', concept_table, observation_concept_id.strip(), observation_concept_id.strip(), person_table, person_table, observation_table, 'observation_concept_id', observation_concept_id.strip()))
