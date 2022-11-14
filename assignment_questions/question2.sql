
--assumptions
---- price in table is per unit of the product
---- since there is no unique order id in the table, considering each row to be associated with an unique order

select avg( taxable_amount + (taxable_amount * tax_rate) + (taxable_amount * shipping_rate ) )  as average_final_order_cost
from
  (
    select price*quantity as taxable_amount, tax_rate, shipping_rate
    from orders
  )

---- this query should return a scaler value which is average order cost
