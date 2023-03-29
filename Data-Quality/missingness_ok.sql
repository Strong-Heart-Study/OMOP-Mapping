select distinct 'Demographics' as 'domain', 'Gender' as 'concept_name', null as 'concept_id', (select count(*) from person where gender_concept_id not in(8507, 8532)) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person
union all
select distinct 'Demographics' as 'domain', 'Date of Birth' as 'concept_name', null as 'concept_id', (select count(*) from person where birth_datetime is null) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person
union all
select distinct 'Demographics' as 'domain', 'Date of Death' as 'concept_name', null as 'concept_id', (select count(*) from person where death_datetime is null) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 132797) as 'concept_name', 132797 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 132797)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 134057) as 'concept_name', 134057 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 134057)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 193253) as 'concept_name', 193253 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 193253)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 193782) as 'concept_name', 193782 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 193782)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 201820) as 'concept_name', 201820 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 201820)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 255573) as 'concept_name', 255573 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 255573)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 312327) as 'concept_name', 312327 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 312327)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 315286) as 'concept_name', 315286 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 315286)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 319835) as 'concept_name', 319835 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 319835)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 319844) as 'concept_name', 319844 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 319844)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 320128) as 'concept_name', 320128 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 320128)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 321318) as 'concept_name', 321318 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 321318)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 373503) as 'concept_name', 373503 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 373503)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 439392) as 'concept_name', 439392 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 439392)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 439727) as 'concept_name', 439727 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 439727)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 443597) as 'concept_name', 443597 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 443597)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 443601) as 'concept_name', 443601 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 443601)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 443611) as 'concept_name', 443611 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 443611)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 443612) as 'concept_name', 443612 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 443612)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 443614) as 'concept_name', 443614 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 443614)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4046363) as 'concept_name', 4046363 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 4046363)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4055678) as 'concept_name', 4055678 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 4055678)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4064161) as 'concept_name', 4064161 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 4064161)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4193704) as 'concept_name', 4193704 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 4193704)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4201478) as 'concept_name', 4201478 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 4201478)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4311629) as 'concept_name', 4311629 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 4311629)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4329847) as 'concept_name', 4329847 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 4329847)
union all
select 'Condition' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 46271022) as 'concept_name', 46271022 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.condition_occurrence where condition_concept_id = 46271022)
union all
select 'Drug' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 21600381) as 'concept_name', 21600381 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.drug_exposure where drug_concept_id = 21600381)
union all
select 'Drug' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 21600712) as 'concept_name', 21600712 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.drug_exposure where drug_concept_id = 21600712)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3000034) as 'concept_name', 3000034 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3000034)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3000905) as 'concept_name', 3000905 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3000905)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3000963) as 'concept_name', 3000963 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3000963)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3004249) as 'concept_name', 3004249 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3004249)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3004295) as 'concept_name', 3004295 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3004295)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3004410) as 'concept_name', 3004410 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3004410)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3005424) as 'concept_name', 3005424 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3005424)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3006906) as 'concept_name', 3006906 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3006906)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3006923) as 'concept_name', 3006923 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3006923)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3007070) as 'concept_name', 3007070 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3007070)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3008342) as 'concept_name', 3008342 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3008342)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3008364) as 'concept_name', 3008364 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3008364)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3009413) as 'concept_name', 3009413 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3009413)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3010156) as 'concept_name', 3010156 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3010156)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3011904) as 'concept_name', 3011904 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3011904)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3012888) as 'concept_name', 3012888 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3012888)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3013721) as 'concept_name', 3013721 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3013721)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3014576) as 'concept_name', 3014576 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3014576)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3014791) as 'concept_name', 3014791 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3014791)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3015632) as 'concept_name', 3015632 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3015632)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3016244) as 'concept_name', 3016244 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3016244)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3016407) as 'concept_name', 3016407 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3016407)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3016723) as 'concept_name', 3016723 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3016723)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3017250) as 'concept_name', 3017250 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3017250)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3019550) as 'concept_name', 3019550 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3019550)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3020416) as 'concept_name', 3020416 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3020416)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3020460) as 'concept_name', 3020460 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3020460)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3020630) as 'concept_name', 3020630 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3020630)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3022192) as 'concept_name', 3022192 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3022192)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3023053) as 'concept_name', 3023053 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3023053)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3023103) as 'concept_name', 3023103 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3023103)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3023314) as 'concept_name', 3023314 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3023314)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3024128) as 'concept_name', 3024128 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3024128)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3024561) as 'concept_name', 3024561 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3024561)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3024731) as 'concept_name', 3024731 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3024731)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3024929) as 'concept_name', 3024929 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3024929)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3025315) as 'concept_name', 3025315 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3025315)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3025673) as 'concept_name', 3025673 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3025673)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3027114) as 'concept_name', 3027114 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3027114)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3028288) as 'concept_name', 3028288 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3028288)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3028437) as 'concept_name', 3028437 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3028437)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3030354) as 'concept_name', 3030354 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3030354)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3032843) as 'concept_name', 3032843 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3032843)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3034485) as 'concept_name', 3034485 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3034485)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3035529) as 'concept_name', 3035529 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3035529)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3036277) as 'concept_name', 3036277 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3036277)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3037110) as 'concept_name', 3037110 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3037110)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3037556) as 'concept_name', 3037556 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3037556)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3038553) as 'concept_name', 3038553 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3038553)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3049718) as 'concept_name', 3049718 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 3049718)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4087498) as 'concept_name', 4087498 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 4087498)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4108289) as 'concept_name', 4108289 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 4108289)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4314539) as 'concept_name', 4314539 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 4314539)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 40764999) as 'concept_name', 40764999 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 40764999)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 40789150) as 'concept_name', 40789150 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 40789150)
union all
select 'Measurement' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 46237026) as 'concept_name', 46237026 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.measurement where measurement_concept_id = 46237026)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 3027199) as 'concept_name', 3027199 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 3027199)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4036083) as 'concept_name', 4036083 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4036083)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4042875) as 'concept_name', 4042875 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4042875)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4058286) as 'concept_name', 4058286 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4058286)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4058709) as 'concept_name', 4058709 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4058709)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4059317) as 'concept_name', 4059317 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4059317)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4059356) as 'concept_name', 4059356 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4059356)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4059475) as 'concept_name', 4059475 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4059475)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4076114) as 'concept_name', 4076114 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4076114)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4086751) as 'concept_name', 4086751 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4086751)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4087501) as 'concept_name', 4087501 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4087501)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4093982) as 'concept_name', 4093982 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4093982)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4111665) as 'concept_name', 4111665 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4111665)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4144036) as 'concept_name', 4144036 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4144036)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4172830) as 'concept_name', 4172830 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4172830)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4269436) as 'concept_name', 4269436 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4269436)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 4307859) as 'concept_name', 4307859 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 4307859)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 35610375) as 'concept_name', 35610375 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 35610375)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 36660024) as 'concept_name', 36660024 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 36660024)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 37392374) as 'concept_name', 37392374 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 37392374)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 37393351) as 'concept_name', 37393351 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 37393351)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 40766362) as 'concept_name', 40766362 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 40766362)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 40769277) as 'concept_name', 40769277 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 40769277)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 42528764) as 'concept_name', 42528764 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 42528764)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 44805441) as 'concept_name', 44805441 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 44805441)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 46270078) as 'concept_name', 46270078 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 46270078)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000002) as 'concept_name', 2000000002 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000002)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000003) as 'concept_name', 2000000003 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000003)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000004) as 'concept_name', 2000000004 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000004)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000005) as 'concept_name', 2000000005 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000005)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000007) as 'concept_name', 2000000007 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000007)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000008) as 'concept_name', 2000000008 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000008)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000009) as 'concept_name', 2000000009 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000009)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000010) as 'concept_name', 2000000010 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000010)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000012) as 'concept_name', 2000000012 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000012)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000014) as 'concept_name', 2000000014 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000014)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000015) as 'concept_name', 2000000015 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000015)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000016) as 'concept_name', 2000000016 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000016)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000017) as 'concept_name', 2000000017 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000017)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000018) as 'concept_name', 2000000018 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000018)
union all
select 'Observation' as 'domain', (select concept_name from omop1.dbo.concept where concept_id = 2000000019) as 'concept_name', 2000000019 as 'concept_id', count(person_id) as 'persons_missing', (select count(*) from omop1.dbo.person) as 'persons_total' from omop1.dbo.person where person_id not in (select distinct person_id from omop1.dbo.observation where observation_concept_id = 2000000019)
