{{ config(materialized='view') }}
-- One row per non-NULL tag, for the model-declared customer_tags semantic model.
select concat(c.id, ':', t.ord) as id, c.id as customer_id, c.region_id, t.tag
from {{ source('raw', 'customers') }} c, unnest(c.tags) with ordinality t(tag, ord)
where t.tag is not null
