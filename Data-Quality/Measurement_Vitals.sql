--Vitals data quality checks 


use omoprawdata
go

if object_id('tempdb.dbo.#Data_Quality_Measurement') is not null drop table #Data_Quality_Measurement
create table #Data_Quality_Measurement (
	variable_num int
	, variable_name varchar(50)
	, source_variable_num int
	, source_variable varchar(50)
)

insert into #Data_Quality_Measurement (variable_num, variable_name, source_variable_num, source_variable) 
select 1, 'Systolic Blood Pressure', 1, 'S1SBP'
union all 
select 1, 'Systolic Blood Pressure', 2, 'S2SBP'
union all 
select 1, 'Systolic Blood Pressure', 3, 'S3SBP'
union all 
select 2, 'Diastolic Blood Pressure', 1, 'S1DBP'
union all 
select 2, 'Diastolic Blood Pressure', 2, 'S2DBP'
union all 
select 2, 'Diastolic Blood Pressure', 3, 'S3DBP'
union all 
select 3, 'Weight', 1, 'S1WT'
union all 
select 3, 'Weight', 2, 'S2WT'
union all 
select 3, 'Weight', 3, 'S3WT'
union all 
select 4, 'Height',  1,'S1HT'
union all 
select 4, 'Height', 2, 'S2HT'
union all 
select 4, 'Height', 3, 'S3HT'
union all 
select 5, 'Body Mass Index', 1, 'S1BMI'
union all 
select 5, 'Body Mass Index', 2, 'S2BMI'
union all 
select 5, 'Body Mass Index', 3, 'S3BMI'
union all 
select 6, 'Body Surface Area', 1, 'S1BSA'
union all 
select 7, 'Mean Arterial Pressure', 1, 'S1MBP'
union all 
select 7, 'Mean Arterial Pressure', 2, 'S2MBP'
union all 
select 7, 'Mean Arterial Pressure', 3, 'S3MBP'
union all 
select 8, 'Palse Pressure', 1, 'S1PP'
union all 
select 8, 'Palse Pressure', 2, 'S2PP'
union all 
select 8, 'Palse Pressure', 3, 'S3PP'
union all 
select 9, '% Body Fat', 1, 'S1BDFAT'
union all 
select 9, '% Body Fat', 2, 'S2BDFAT'
union all 
select 9, '% Body Fat', 3, 'S3BDFAT'


select * from #Data_Quality_Measurement


if object_id('tempdb.dbo.#Data_Quality_Measurement_check') is not null drop table #Data_Quality_Measurement_check
create table #Data_Quality_Measurement_check (
	variable_num int
	, variable_name varchar(50)
	, source_counts int 
	, destination_counts int 
	, mismatch_YN varchar(1)
)

insert into #Data_Quality_Measurement_check (variable_num, variable_name)
select distinct variable_num, variable_name from #Data_Quality_Measurement

select * from #Data_Quality_Measurement_check 




-------------
declare @i as integer = 1, @j integer = 1, @src_count integer = 0, @count integer = 0;
DECLARE @SQL nvarchar(MAX), @var1 nvarchar(max), @ParamDefinition nvarchar(500);


while  (@i <= (select count(distinct variable_name) from #Data_Quality_Measurement))
begin 
	while (@j <= (select count(distinct source_variable) from #Data_Quality_Measurement where variable_num = @i))
	begin
	--source table counts
		set @var1 = (select source_variable from #Data_Quality_Measurement where variable_num = @i and source_variable_num = @j);
				
		SET @SQL = N'SELECT @tbl_count = COUNT(*) FROM omoprawdata.dbo.SHSALL33 where ' + convert(nvarchar(30), @var1) + N' is not null;';
		SET @ParamDefinition = N'@tbl_count int OUTPUT'; 
		EXEC sp_executesql @SQL, @ParamDefinition,  @tbl_count =  @count OUTPUT;
		set @src_count = @src_count + @count; 
	
		set @j = @j + 1;
		set @count = 0;
	end 

	--Update source table counts 
	update #Data_Quality_Measurement_check set source_counts = @src_count where variable_num = @i;

	--reset counters and counts
	set @j = 1
	set @src_count = 0

	--update destination table counts
	update #Data_Quality_Measurement_check
	set destination_counts = (select count(*)
		from omop1.dbo.measurement ms 
		join omop1.dbo.source_to_concept_map sc on sc.target_concept_id = ms.measurement_concept_id
			and sc.source_code = ms.measurement_source_value
		where sc.source_code in (select source_variable from #Data_Quality_Measurement where variable_num = @i))
	where variable_num = @i ;
	set @i = @i + 1
end 


--update mismatch column
update #Data_Quality_Measurement_check
set mismatch_YN = case 
	when source_counts = destination_counts then 'Y' 
	else 'N' end 

----------------


select * from #Data_Quality_Measurement_check


