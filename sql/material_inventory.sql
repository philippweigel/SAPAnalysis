CREATE TABLE sap_staging.material_inventory (
    client_id text,
    material text,
    plant_code text,
    storage_location text,
    unrestricted_stock_qty DECIMAL(15, 3),
    blocked_stock_returns_qty DECIMAL(15, 3),
    created_on timestamp
);



INSERT INTO sap_staging.material_inventory
with material_inventory as(
	select 
		mandt,
		matnr,
		werks,
		lgort,
		CAST(labst AS NUMERIC) AS labst,
		CAST(retme AS NUMERIC) AS retme,
		CAST(recordstamp as TIMESTAMP) as recordstamp
	from sap_raw.mard
)
select *
from material_inventory
order by
		mandt,
		matnr,
		werks,
		lgort,
		recordstamp
