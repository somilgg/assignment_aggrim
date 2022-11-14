--assumptions
-- considering product table schemma to be this
--Region, Crop name, Crop SKU, and Selling price, Last Modified Time, Product_active_status

-- this query will give latest price for active products and last traded price from order table for inactive products

with active_products as
(
    select *, row_number() over (partition by crop_SKU order by LastModifiedTime desc) as r
    from product_table
    where Product_active_status = "Active"
),

latest_price_of_active_products as
(
    select Cropname, Crop_SKU, Selling_price as price
    from active_products
    where r = 1
),

inactive_products as
(
    select distinct Crop_SKU, Cropname
    from product_table
    where Product_active_status = "InActive"
),

last_traded_price_inactive_products as
(
 select Cropname, Crop_SKU, price
 from
     (
     select inactive_products.Cropname, inactive_products.Crop_SKU, orders_table.price, orders_table.date,
     row_number() over (partition by Crop_SKU order by date desc) as r
     from
     orders_table
     inner join
     inactive_products
     on inactive_products.Crop_SKU  = orders_table.product_sku
     )
     where r = 1
)

select *
from
latest_price_of_active_products
union all
last_traded_price_inactive_products