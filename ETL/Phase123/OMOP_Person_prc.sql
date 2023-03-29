/********************************************************************
Name: ETL to populate OMOP Person Table

Source Data tables: SHS Phase123
					SOURCE_TO_CONCEPT_MAP

Destination table: PERSON

Instructions:
	1) Import the completed source_to_concept_map.xlsx file
	
	2) Change the name of the DB to the appropriate database

	3) Change the name of the table 'strongheart.patient' 
		to the appropriate table that contains SHS Phase123 data

	4) Change OMOP_VOCABULARY.vocab51.source_to_concept_map to the 
		appropriate db/table name.

	5) If Race and/or Ethnicity values are unavailable, delete the 
		last 2 left joins

	6) If Race and/or Ethnicity values are avaiable, replace 
		, 0 as race_concept_id	
		, 0 as ethnicity_concept_id
		
		to
		, COALESCE(race.target_concept_id, 0) as race_concept_id
		, COALESCE(ethnicity.target_concept_id, 0) as ethnicity_concept_id

		AND
		
		add the source (raw) race and ethnicity values to the following columns:
		* race_source_value
		* ethnicity_source_value		 


*********************************************************************/

	
	use strongHeart_OMOP 
	go


	insert into dbo.person (
	   person_id
	 , gender_concept_id
	 , year_of_birth
	 , month_of_birth
	 , day_of_birth
	 , birth_datetime
	 --, death_datetime
	 , race_concept_id
	 , ethnicity_concept_id
	 , location_id
	 , provider_id
	 , care_site_id
	 , person_source_value
	 , gender_source_value
	 , gender_source_concept_id 
	 , race_source_value
	 , race_source_concept_id
	 , ethnicity_source_value
	 , ethnicity_source_concept_id
	 )

	select pat.[IDNO] as person_id
	, COALESCE(gender.target_concept_id, 0) as gender_concept_id
	, COALESCE( year(S1EXDATE) - S1AGE 
			  , year(S2EXDATE) - S2AGE
			  , year(S3EXDATE) - S3AGE ) as year_of_birth
	, 1 as month_of_birth
	, 1 as day_of_birth
	, cast(COALESCE( year(S1EXDATE) - S1AGE 
			  , year(S2EXDATE) - S2AGE
			  , year(S3EXDATE) - S3AGE ) 
			  as varchar(20)) + '-01-01' as birth_datetime
	--, NULL as death_datetime			--- change if death date is available
	, race.concept_id as race_concept_id			
	, 0 as ethnicity_concept_id			-- change if ethnicity is available
	, l.location_id as location_id
	, 1 as provider_id
	, c.care_site_id as care_site_id
	, IDNO as person_source_value
	, pat.sex as gender_source_value
	, 0 as gender_source_concept_id
	, 'American Indian' as race_source_value			
	, 0 as race_source_concept_id
	, NULL as ethnicity_source_value 
	, 0 as ethnicity_source_concept_id -- change if ethnicity is available


	from omoprawdata.dbo.SHSALL33 pat --raw source data
	left join omop1.dbo.source_to_concept_map gender on gender.source_code = pat.sex
		and gender.source_vocabulary_id = 'Gender'
	
	left join omop1.dbo.CONCEPT race on race.vocabulary_id = 'Race' 
		and race.standard_concept = 'S'
		and race.concept_name = 'American Indian'

--possible race values:
--8657	American Indian or Alaska Native
--38003572	American Indian
		
	----Delete/ Comment out the following statements if race and ethnicity are unavailable 
	--left join OMOP_VOCABULARY.vocab51.source_to_concept_map ethnicity on ethnicity.source_code = pat.BBBB
	--	and ethnicity.source_vocabulary_id = 'Ethnicity'

	left join omop1.dbo.[location] l on l.location_source_value = pat.Center
	left join omop1.dbo.care_site c on c.care_site_source_value = pat.Center

	

------Update PERSON table to add Race values
----update dbo.person 
----set race_concept_id = (select target_concept_id from dbo.source_to_concept_map where source_code_description = 'American Indian')
----, race_source_value = (select source_code +':' + source_code_description from dbo.source_to_concept_map where source_code_description = 'American Indian')
----, race_source_concept_id = 0



------Update PERSON table to add location and care-site values
update p
set p.location_id = l.location_id, p.care_site_id = c.care_site_id
--select p.person_id, shs.CENTER, l.location_id, l.location_source_value,  c.care_site_id, c.care_site_source_value
from omop1.dbo.person p 
join omoprawdata.dbo.SHSALL33 shs on shs.IDNO = p.person_id 
left join omop1.dbo.location l on l.location_source_value = shs.CENTER 
left join omop1.dbo.care_site c on c.care_site_source_value = shs.CENTER 



