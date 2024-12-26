
CREATE TABLE sap_staging.billing_documents (
    client_id text,
    billing_doc_number text,
    billing_doc_item_number text,
    billed_qty numeric,
    material text,
    net_value numeric,
    billing_date date,
    customer_number text
);



INSERT INTO sap_staging.billing_documents
with header as (
	select 
		*,
		ROW_NUMBER() OVER (
	        PARTITION BY mandt, vbeln
	        ORDER BY recordstamp DESC
	    ) AS row_num
	from sap_raw.vbrk
)
select
	v.mandt,
	v.vbeln,
	v.posnr,
	cast(v.fkimg as numeric) as fkimg,
	v.matnr,
	cast(v.netwr as numeric) as netwr,
	cast(header.fkdat as date) as fkdat,
	header.kunrg
from sap_raw.vbrp v
left join header
on 
	v.mandt = header.mandt and 
	v.vbeln = header.vbeln and 
	header.row_num = 1
order by 1,2,3
