select
    'condition_occurrence' as OMOP_Table,
	(select count(*) from omop1.dbo.condition_occurrence) as row_Count,
	(select count(distinct person_id) from omop1.dbo.condition_occurrence) as personCount,
    (select count(distinct condition_concept_id) from omop1.dbo.condition_occurrence) as conceptCount,
    null as conceptAndQualifierCount,
    avg(a.numRows) as meanRowsPerPerson, min(a.numRows) as minRowsPerPerson, max(a.numRows) as maxRowsPerPerson,STDEV(a.numRows) as stdevRowsPerPerson from (
        select person_id, count(*) as numRows from omop1.dbo.condition_occurrence group by person_id
    ) a
union
select
    'drug_exposure' as OMOP_Table,
	(select count(*) from omop1.dbo.drug_exposure) as row_Count,
	(select count(distinct person_id) from omop1.dbo.drug_exposure) as personCount,
    (select count(distinct drug_concept_id) from omop1.dbo.drug_exposure) as conceptCount,
    null as conceptAndQualifierCount,
    avg(a.numRows) as meanRowsPerPerson, min(a.numRows) as minRowsPerPerson, max(a.numRows) as maxRowsPerPerson,STDEV(a.numRows) as stdevRowsPerPerson from (
        select person_id, count(*) as numRows from omop1.dbo.drug_exposure group by person_id
    ) a
union
select
    'measurement' as OMOP_Table,
	(select count(*) from omop1.dbo.measurement) as row_Count,
	(select count(distinct person_id) from omop1.dbo.measurement) as personCount,
    (select count(distinct measurement_concept_id) from omop1.dbo.measurement) as conceptCount,
    (select count(distinct concat(measurement_concept_id, unit_concept_id)) from omop1.dbo.measurement) as conceptAndQualifierCount,
    avg(a.numRows) as meanRowsPerPerson, min(a.numRows) as minRowsPerPerson, max(a.numRows) as maxRowsPerPerson,STDEV(a.numRows) as stdevRowsPerPerson from (
        select person_id, count(*) as numRows from omop1.dbo.measurement group by person_id
    ) a
union
select
    'observation' as OMOP_Table,
	(select count(*) from omop1.dbo.observation) as row_Count,
	(select count(distinct person_id) from omop1.dbo.observation) as personCount,
    (select count(distinct observation_concept_id) from omop1.dbo.observation) as conceptCount,
    (select count(distinct concat(observation_concept_id, qualifier_concept_id)) from omop1.dbo.observation) as conceptAndQualifierCount,
    avg(a.numRows) as meanRowsPerPerson, min(a.numRows) as minRowsPerPerson, max(a.numRows) as maxRowsPerPerson,STDEV(a.numRows) as stdevRowsPerPerson from (
        select person_id, count(*) as numRows from omop1.dbo.observation group by person_id
    ) a
union
select
    'person' as OMOP_Table,
	(select count(*) from omop1.dbo.person) as row_Count,
	(select count(distinct person_id) from omop1.dbo.person) as personCount,
    null as conceptCount,
    null as conceptAndQualifierCount,
    avg(a.numRows) as meanRowsPerPerson, min(a.numRows) as minRowsPerPerson, max(a.numRows) as maxRowsPerPerson,STDEV(a.numRows) as stdevRowsPerPerson from (
        select person_id, count(*) as numRows from omop1.dbo.person group by person_id
    ) a
union
select
    'visit' as OMOP_Table,
	(select count(*) from omop1.dbo.visit_occurrence) as row_Count,
	(select count(distinct person_id) from omop1.dbo.visit_occurrence) as personCount,
    null as conceptCount,
    null as conceptAndQualifierCount,
    avg(a.numRows) as meanRowsPerPerson, min(a.numRows) as minRowsPerPerson, max(a.numRows) as maxRowsPerPerson,STDEV(a.numRows) as stdevRowsPerPerson from (
        select person_id, count(*) as numRows from omop1.dbo.visit_occurrence group by person_id
    ) a
