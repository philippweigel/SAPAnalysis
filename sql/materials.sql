CREATE TABLE sap_staging.materials (
    client_id text,
    material text,
    material_type text,
    material_group text,
    gross_weight DECIMAL(15, 3),
    net_weight DECIMAL(15, 3),
    weight_unit text,
    volume DECIMAL(15, 3),
    volume_unit text,
    material_short_desc text,
    material_long_desc text
);



INSERT INTO sap_staging.materials
with material_master as (
select
	mandt,
	matnr,
	mtart,
	matkl,
 	CAST(brgew AS NUMERIC) AS brgew,
    CAST(ntgew AS NUMERIC) AS ntgew,
	gewei,
	CAST(volum AS NUMERIC) AS volum,
	voleh
from sap_raw.mara m
group by
	mandt,
	matnr,
	mtart,
	matkl,
	brgew,
	ntgew,
	gewei,
	volum,
	voleh
order by 1,2 )
,
material_description as (
		select 
		mandt,
		matnr,
		maktx,
		maktg
	from sap_raw.makt
	where spras = 'E'
	order by 1,2
)
select 
	mm.*,
	md.maktx,
	md.maktg
from material_master mm
left join material_description md
on mm.mandt = md.mandt and mm.matnr = md.matnr

	
