---Add value_as_concept_ids

-- S4KD 
begin 
	--update o set value_as_concept_id = 4188539 --Yes
	select * 
	from omop1.dbo.observation o
	where observation_source_value = 'S4KD = Y'
	and observation_concept_id = 40766362

	--update o set value_as_concept_id = 4188540 --No
	select * 
	from omop1.dbo.observation	o
	where observation_source_value = 'S4KD = N'
	and observation_concept_id = 40766362

end 


/*
2000000009	0	0	S3HTNRX = N
2000000009	0	0	S5HTNRX = Y
*/
begin
	--updat o set value_as_concept_id = 4188539 --Yes
	select * 
	from omop1.dbo.observation o
	where observation_source_value = 'S3HTNRX = Y'
	and observation_concept_id = 2000000009

	--update o set value_as_concept_id = 4188540 --No
	select * 
	from omop1.dbo.observation	o
	where observation_source_value = 'S3HTNRX = N'
	and observation_concept_id = 2000000009
end


/*
40766362	0	0	S3SMOKE = N
40766362	0	0	S2SMOKE = Y
*/
begin
	--update o set value_as_concept_id = 4188539 --Yes
	select * 
	from omop1.dbo.observation o
	where observation_source_value = 'S2SMOKE = Y'
	and observation_concept_id = 40766362

	--update o set value_as_concept_id = 4188540 --No
	select * 
	from omop1.dbo.observation	o
	where observation_source_value = 'S3SMOKE = N'
	and observation_concept_id = 40766362
end
