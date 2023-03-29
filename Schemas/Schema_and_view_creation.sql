use omop1


-- create schema SHS_APP for the application to access consented Strongheart participants
create schema SHS_APP


-- create views for the SHS_APP schema

--care_site 
create view SHS_APP.care_site as 
	select p.*
	from omop1.dbo.care_site p

--location 
create view SHS_APP.[location] as 
	select p.*
	from omop1.dbo.[location] p

--provider 
create view SHS_APP.[provider] as 
	select p.*
	from omop1.dbo.[provider] p


--person 
create view SHS_APP.person as 
	select p.*
	from omop1.dbo.person p
	join omop1.dbo.cohort c  on c.subject_id = p.person_id 
	where c.cohort_definition_id = 1 -- currently eligible participants 

--visit_occurrence 
create view SHS_APP.visit_occurrence as 
	select p.*
	from omop1.dbo.visit_occurrence p
	join omop1.dbo.cohort c  on c.subject_id = p.person_id 
	where c.cohort_definition_id = 1 -- currently eligible participants 

--condition_occurrence 
create view SHS_APP.condition_occurrence as 
	select p.*
	from omop1.dbo.condition_occurrence p
	join omop1.dbo.cohort c  on c.subject_id = p.person_id 
	where c.cohort_definition_id = 1 -- currently eligible participants 


--drug_exposure 
create view SHS_APP.drug_exposure as 
	select p.*
	from omop1.dbo.drug_exposure p
	join omop1.dbo.cohort c  on c.subject_id = p.person_id 
	where c.cohort_definition_id = 1 -- currently eligible participants 

--visit_occurrence 
create view SHS_APP.procedure_occurrence as 
	select p.*
	from omop1.dbo.procedure_occurrence p
	join omop1.dbo.cohort c  on c.subject_id = p.person_id 
	where c.cohort_definition_id = 1 -- currently eligible participants 

--measurement 
create view SHS_APP.measurement as 
	select p.*
	from omop1.dbo.measurement p
	join omop1.dbo.cohort c  on c.subject_id = p.person_id 
	where c.cohort_definition_id = 1 -- currently eligible participants 


--observation 
create view SHS_APP.observation as 
	select p.*
	from omop1.dbo.observation p
	join omop1.dbo.cohort c  on c.subject_id = p.person_id 
	where c.cohort_definition_id = 1 -- currently eligible participants 