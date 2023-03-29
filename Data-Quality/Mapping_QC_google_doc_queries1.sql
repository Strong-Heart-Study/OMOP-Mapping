use omop1

-- # of patients (source)
Begin 
	drop table if exists #SHS_Patients 
	select 'SHS' source, 'patients' variable, cast('' as varchar(10)) [value]
	, count(distinct a.idno) patient_counts, NULL visit_counts into #SHS_Patients
	from (
		select distinct idno from omoprawdata.dbo.SHSALL33 union 
		select distinct idno from omoprawdata.dbo.S4ALL23 union  
		select distinct idno from omoprawdata.dbo.S5ALL23  union 
		select distinct idno from omoprawdata.dbo.s6all union 
		select distinct idno from omoprawdata.dbo.CCVD2016ALL  union 
		select distinct idno from omoprawdata.dbo.FCVD2016ALL  
		)a

	union 

	-- # of patients (destination)
	select 'OMOP' source, 'patients' variable, cast('' as varchar(10)) [value]
	, count(distinct person_id) patient_counts, NULL visit_counts 
	from dbo.person
End 

---------------------------------------------------------------

-- # of visits (source)
Begin 
	drop table if exists #SHS_Visits 
	select 'SHS' source, 'visits' variable, cast('' as varchar(10)) [value], 'Phase I' phase, count(distinct idno) patient_counts, count(*) visit_counts into #SHS_Visits
	from omoprawdata.dbo.SHSALL33 where s1exdate is not null -- phase 1
	union
	select 'SHS' source, 'visits' variable, cast('' as varchar(10)) [value], 'Phase II' phase, count(distinct idno) patient_counts, count(*) visit_counts
	from omoprawdata.dbo.SHSALL33 where S2EXDATE is not null -- phase 2
	union
	select 'SHS' source, 'visits' variable, cast('' as varchar(10)) [value], 'Phase III' phase, count(distinct idno) patient_counts, count(*) visit_counts
	from omoprawdata.dbo.SHSALL33 where S3EXDATE is not null -- phase 3
	union
	select 'SHS' source, 'visits' variable, cast('' as varchar(10)) [value], 'Phase IV' phase, count(distinct idno) patient_counts, count(*) visit_counts
	from omoprawdata.dbo.S4ALL23 where S4EXDATE is not null -- phase 4
	union
	select 'SHS' source, 'visits' variable, cast('' as varchar(10)) [value], 'Phase V' phase, count(distinct idno) patient_counts, count(*) visit_counts
	from omoprawdata.dbo.S5ALL23 where S5EXDATE is not null -- phase 5
	union
	select 'SHS' source, 'visits' variable, cast('' as varchar(10)) [value], 'Phase VI' phase, count(distinct idno) patient_counts, count(*) visit_counts
	from omoprawdata.dbo.s6all where S6EXDATE is not null -- phase 2

	 union 

	-- # of visits (destination)
	select 'OMOP' source, 'visits' variable, cast('' as varchar(10)) [value]
		, replace(left(visit_source_value, len(visit_source_value)-4), 'SHS ', '') phase
		, count(distinct person_id) patient_counts, count(*) visit_counts 
	from dbo.visit_occurrence vo 
	group by replace(left(visit_source_value, len(visit_source_value)-4), 'SHS ', '')

End 
 
---------------------

-- # of female patients (source)
Begin 
	drop table if exists #SHS_Gender 
	select 'SHS' source, 'Gender' variable, a.SEX [value]
	, count(distinct a.idno) patient_counts, count(*) record_counts into #SHS_Gender
	from (
		select  idno, SEX from omoprawdata.dbo.SHSALL33 union 
		select  idno, SEX from omoprawdata.dbo.SHSALL33 union  
		select  idno, SEX from omoprawdata.dbo.SHSALL33 union 
		select  idno, SEX from omoprawdata.dbo.SHSALL33 union 
		select  idno, SEX from omoprawdata.dbo.SHSALL33 union 
		select  idno, SEX from omoprawdata.dbo.SHSALL33  
		)a
	group by a.SEX

	union 

	-- # of female patients (destination)
	select 'OMOP' source, 'Gender' variable, gender_source_value [value]
	, count(distinct person_id) patient_counts, count(*) records_counts 
	from dbo.person
	group by gender_source_value
END 
---------------------------------------------------------------------------------------
--Age
Begin 
	/*
	Age (Mean)
	Age group [0-20] count
	Age group [21- 45] count
	Age group [46-65] count
	Age group [66-80] count
	Age group[> 80] count



	*/
	-- SHS variables
	drop table if exists #SHS_age
	select *, case
	when current_age between 0 and 20 then '0 -20'
	when current_age between 21 and 45 then '21 -45'
	when current_age between 46 and 65 then '46 -65'
	when current_age between 66 and 80 then '66 -80'
	when current_age >80 then '>80'
	end age_range
	into #SHS_age from (
		select *, year(getdate()) - birth_year current_age, ROW_NUMBER() over (partition by idno order by birth_year asc) rownum 
		from (
			select  idno, year(s1exdate) - S1AGE birth_year from omoprawdata.dbo.SHSALL33
			union 
			select idno, year(s2exdate) - S2AGE from omoprawdata.dbo.SHSALL33
			union 
			select idno, year(s3exdate) - S3AGE from omoprawdata.dbo.SHSALL33
			union 
			select idno, year(s4exdate) - S4AGE from omoprawdata.dbo.S4ALL23
			union 
			select idno, year(s5exdate) - S5AGE from omoprawdata.dbo.S5ALL23
			union 
			select idno, year(DOB) from omoprawdata.dbo.s6all
		) a
	) b 


	--select * from #SHS_age

	-- OMOP 
	drop table if exists #OMOP_age
	select person_id, year(getdate()) - year_of_birth current_age, case
	when year(getdate()) - year_of_birth between 0 and 20 then '0 -20'
	when year(getdate()) - year_of_birth between 21 and 45 then '21 -45'
	when year(getdate()) - year_of_birth between 46 and 65 then '46 -65'
	when year(getdate()) - year_of_birth between 66 and 80 then '66 -80'
	when year(getdate()) - year_of_birth >80 then '>80'
	end age_range
	into #OMOP_age
	from dbo.person
	where death_datetime is null -- alive 

	--select * from #OMOP_age

end 
---------------------------------------------------------------------------------------
-- BMI
Begin
/*

SHS variables
S1BMI
S2BMI
S3BMI
S4BMI
S5BMI
S6BMI


OMOP concept_id = 3038553

*/


	drop table if exists #BMI
	select 'SHS' source, 'BMI' variable, count(*) counts, avg(BMI) [mean_value], STDEV(BMI) stdev_value
	into #BMI
	from (
		select idno, bmi from (
			select idno, BMI, ROW_NUMBER() over (partition by idno order by exdate desc) rownum
			from (
				select idno, S1BMI BMI, S1EXDATE exdate  from omoprawdata.dbo.SHSALL33 where S1BMI is not null 
				union 
				select idno, S2BMI, S2EXDATE from omoprawdata.dbo.SHSALL33 where S2BMI is not null 
				union 
				select idno, S3BMI, S3EXDATE from omoprawdata.dbo.SHSALL33 where S3BMI is not null 
				union 
				select idno, S4BMI, S4EXDATE from omoprawdata.dbo.S4ALL23 where S4BMI is not null 
				union 
				select idno, S5BMI, S5EXDATE from omoprawdata.dbo.S5ALL23 where S5BMI is not null 
				union 
				select idno, S6BMI, S6EXDATE from omoprawdata.dbo.s6all where S6BMI is not null 
			) a 
		) b where rownum = 1	--latest BMI
	) c group by BMI


	union

	-- OMOP Hypertension counts
	select 'OMOP' source, 'BMI' variable, count(*) counts, avg(value_as_number) mean_value, STDEV(value_as_number) stdev_value 
	from (
		select person_id, measurement_concept_id, value_as_number, measurement_datetime
		, ROW_NUMBER() over (partition by person_id order by measurement_datetime desc) rownum
		from dbo.measurement co 
		where measurement_concept_id = 3038553
	) a where rownum =1 -- latest BMI measurement 
	group by value_as_number

End
---------------------------------------------------------------------------------------

-- Diabetes 
Begin 
/*
source variable list:
S1ADADM
S1WHNDM
S1WHODM
S2ADADM
S2WHNDM
S2WHODM
S3ADADM
S3WHNDM
S3WHODM
S4ADADM
S5ADADM
S6ADADM



OMOP concept_id in ( 2000000003, 2000000004, 2000000007)

*/

-- SHS Hypertension counts

	drop table if exists #DM
	select 'SHS' source, 'DM' variable,  DM [value], count(distinct idno) patient_counts, count(*) record_counts 
	into #DM 
	from (
		select idno, S1ADADM DM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S1WHNDM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S1WHODM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S2ADADM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S2WHNDM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S2WHODM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S3ADADM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S3WHNDM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S3WHODM from omoprawdata.dbo.SHSALL33
		union 
		select idno, S4ADADM from omoprawdata.dbo.S4ALL23
		union 
		select idno, S5ADADM from omoprawdata.dbo.S5ALL23
		union 
		select idno, S6ADADM from omoprawdata.dbo.s6all
	) a 
	group by DM

	union

	-- OMOP Hypertension counts
	select 'OMOP' source, 'DM' variable, value_as_string, count(distinct person_id) patient_count, count(*) record_count 
	from dbo.observation co 
	where observation_concept_id in ( 2000000003, 2000000004, 2000000007)
	group by value_as_string
END


---------------------------------------------------------------------------------------


-- Essential Hypertension
Begin 

/*
source variable list:
S1USHTN2
S2USHTN2
S3USHTN
S3WHOHTN
S4USHTN
S5USHTN
S5WHOHTN


OMOP concept_id in ( 2000000002, 2000000005)

*/

-- SHS Hypertension counts
	drop table if exists #HTN 
	select 'SHS' source, 'HTN' variable,  HTN [value], count(distinct idno) patient_counts, count(*) record_counts 
	into #HTN 
	from (
		select idno, S1USHTN2 HTN from omoprawdata.dbo.SHSALL33
		union 
		select idno, S1WHOHN2 from omoprawdata.dbo.SHSALL33
		union 
		select idno, S2USHTN2 from omoprawdata.dbo.SHSALL33
		union 
		select idno, S2WHOHN2 from omoprawdata.dbo.SHSALL33
		union 
		select idno, S3USHTN from omoprawdata.dbo.SHSALL33
		union 
		select idno, S3WHOHTN from omoprawdata.dbo.SHSALL33
		union 
		select idno, S4USHTN from omoprawdata.dbo.S4ALL23
		--union 
		--select idno, S4WHOHTN from omoprawdata.dbo.S4ALL23
		union 
		select idno, S5USHTN from omoprawdata.dbo.S5ALL23
		union 
		select idno, S5WHOHTN from omoprawdata.dbo.S5ALL23
	) a 
	group by HTN

	union

	-- OMOP Hypertension counts
	select 'OMOP' source, 'HTN' variable, value_as_string, count(distinct person_id) patient_count, count(*) record_count 
	from dbo.observation co 
	where observation_concept_id in ( 2000000002, 2000000005)
	group by value_as_string
END

--------	Results ----------------

-- Patient count 
select * from #SHS_Patients

-- Visit counts
select * from #SHS_Visits

-- Gender
select * from #SHS_Gender

-- Mean Age
select 'SHS' source, 'Mean_age' variable, avg(current_age) mean_age from #SHS_age a
left join omoprawdata.dbo.CCVD2016ALL cvd on cvd.IDNO = a.IDNO
left join omoprawdata.dbo.FCVD2016ALL fcvd on fcvd.IDNO = a.IDNO
where cvd.DOD is null and fcvd.DOD is null -- only alive patients 
and a.rownum = 1	--incase of duplicate data
union
select 'OMOP' source, 'Mean_age' variable, avg(current_age) mean_age from #OMOP_age


-- Age range counts
select 'SHS' source, 'Age_range_counts' variable, age_range, count(*) counts from #SHS_age a
left join omoprawdata.dbo.CCVD2016ALL cvd on cvd.IDNO = a.IDNO
left join omoprawdata.dbo.FCVD2016ALL fcvd on fcvd.IDNO = a.IDNO
where cvd.DOD is null and fcvd.DOD is null -- only alive patients 
and a.rownum = 1	--incase of duplicate data
group by age_range
union
select 'OMOP' source, 'Age_range_counts' variable, age_range, count(*) counts from #OMOP_age
group by age_range


-- BMI 
select * from #BMI

-- Diabetes
select * from #DM 

-- Hypertension
select * from #HTN
