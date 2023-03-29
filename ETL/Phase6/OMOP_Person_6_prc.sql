/********************************************************************
Name: ETL to populate OMOP Person Table

Source Data tables: SHS Phase4
					SOURCE_TO_CONCEPT_MAP

Destination table: PERSON


*********************************************************************/

	
	use omoprawdata 
	go


	insert into omop1.dbo.person (
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
	, year(DOB) as year_of_birth
	, 1 as month_of_birth
	, 1 as day_of_birth
	, DOB as birth_datetime
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

	from omoprawdata.[dbo].s6all pat --raw source data
	left join omop1.dbo.person p1 on p1.person_id = pat.idno

	left join omop1.dbo.source_to_concept_map gender on gender.source_code = pat.sex
		and gender.source_vocabulary_id = 'Gender'
	
	left join omop1.dbo.CONCEPT race on race.vocabulary_id = 'Race' 
		and race.standard_concept = 'S'
		and race.concept_name = 'American Indian'

	left join omop1.dbo.[location] l on l.location_source_value = pat.Center
	left join omop1.dbo.care_site c on c.care_site_source_value = pat.Center

	where p1.person_id is null 
	




	update omop1.dbo.person 
	set birth_datetime = cast(year_of_birth as varchar(10))
	where birth_datetime is null 
	