/*
        Kai Post, October 4, 2022
        We recently notices several participants might BMIs of 0 listed in the Oklahoma database.
*/

select person_id, idno, value_as_number , S6BMI  from omop1.dbo.measurement
join  omoprawdata.dbo.s6all on s6all.idno = measurement.person_id
where measurement_concept_id = 3038553 and value_as_number = 0;

// We checked in the source data and the height there was actually blank for those individuals.
// 21 measurements were deleted, all from phase 6 (S6BMI).
delete from omop1.dbo.measurement where measurement_concept_id = 3038553 and value_as_number = 0;