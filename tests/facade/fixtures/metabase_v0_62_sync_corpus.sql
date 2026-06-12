-- Captured from Metabase v0.62.1.5 syncing a real Postgres 16 with 2 demo tables.
-- 20 distinct SQL statements during sync + GUI question.

-- ============================================================
-- #1 first seen at 2026-06-12 12:40:58.912
-- ============================================================
SET application_name = 'Metabase v0.62.1.5 [d4fe74c4-96c3-4812-b3ae-35e513752b13]';

-- ============================================================
-- #2 first seen at 2026-06-12 12:40:58.932
-- ============================================================
SELECT 1;

-- ============================================================
-- #3 first seen at 2026-06-12 12:40:59.054
-- ============================================================
SHOW TRANSACTION ISOLATION LEVEL;

-- ============================================================
-- #4 first seen at 2026-06-12 12:40:59.055
-- ============================================================
select current_catalog;

-- ============================================================
-- #5 first seen at 2026-06-12 12:40:59.062
-- ============================================================
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

-- ============================================================
-- #6 first seen at 2026-06-12 12:40:59.066
-- ============================================================
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- ============================================================
-- #7 first seen at 2026-06-12 12:40:59.074
-- ============================================================
SELECT nspname AS "TABLE_SCHEM", current_database() AS "TABLE_CATALOG" FROM pg_catalog.pg_namespace  WHERE nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1]) AND (nspname !~ '^pg_toast_temp_'  OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))  ORDER BY "TABLE_SCHEM";

-- ============================================================
-- #8 first seen at 2026-06-12 12:40:59.088
-- ============================================================
with table_privileges as (
	 select
	   NULL as role,
	   t.schemaname as schema,
	   t.objectname as table,
	   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'update') as update,
	   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'select') as select,
	   pg_catalog.has_any_column_privilege(current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'insert') as insert,
	   pg_catalog.has_table_privilege(     current_user, '"' || replace(t.schemaname, '"', '""') || '"' || '.' || '"' || replace(t.objectname, '"', '""') || '"',  'delete') as delete
	 from (
	   select schemaname, tablename as objectname from pg_catalog.pg_tables
	   union
	   select schemaname, viewname as objectname from pg_catalog.pg_views
	   union
	   select schemaname, matviewname as objectname from pg_catalog.pg_matviews
	 ) t
	 where t.schemaname !~ '^pg_'
	   and t.schemaname <> 'information_schema'
	   and pg_catalog.has_schema_privilege(current_user, t.schemaname, 'usage')
	)
	select t.*
	from table_privileges t;

-- ============================================================
-- #9 first seen at 2026-06-12 12:40:59.114
-- ============================================================
SELECT "n"."nspname" AS "schema", "c"."relname" AS "name", CASE "c"."relkind" WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'PARTITIONED TABLE' WHEN 'v' THEN 'VIEW' WHEN 'f' THEN 'FOREIGN TABLE' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE NULL END AS "type", "d"."description" AS "description", NULLIF("stat"."n_live_tup", $1) AS "estimated_row_count" FROM "pg_catalog"."pg_class" AS "c" INNER JOIN "pg_catalog"."pg_namespace" AS "n" ON "c"."relnamespace" = "n"."oid" LEFT JOIN "pg_catalog"."pg_description" AS "d" ON ("c"."oid" = "d"."objoid") AND ("d"."objsubid" = $2) AND ("d"."classoid" = 'pg_class'::regclass) LEFT JOIN "pg_stat_user_tables" AS "stat" ON ("n"."nspname" = "stat"."schemaname") AND ("c"."relname" = "stat"."relname") WHERE ("c"."relnamespace" = "n"."oid") AND ("n"."nspname" !~ $3) AND ("n"."nspname" <> $4) AND c.relkind in ('r', 'p', 'v', 'f', 'm') AND ("n"."nspname" IN ($5)) ORDER BY "type" ASC, "schema" ASC, "name" ASC;

-- ============================================================
-- #10 first seen at 2026-06-12 12:40:59.201
-- ============================================================
show timezone;

-- ============================================================
-- #11 first seen at 2026-06-12 12:40:59.351
-- ============================================================
SELECT nspname, typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.oid IN (SELECT DISTINCT enumtypid FROM pg_enum e);

-- ============================================================
-- #12 first seen at 2026-06-12 12:40:59.387
-- ============================================================
SELECT "c"."column_name" AS "name", CASE WHEN "c"."udt_schema" IN ('public', 'pg_catalog') THEN FORMAT('%s', "c"."udt_name") ELSE FORMAT('"%s"."%s"', "c"."udt_schema", "c"."udt_name") END AS "database-type", "c"."ordinal_position" - 1 AS "database-position", "c"."table_schema" AS "table-schema", "c"."table_name" AS "table-name", "pk"."column_name" IS NOT NULL AS "pk?", COL_DESCRIPTION(CAST(CAST(FORMAT('%I.%I', CAST("c"."table_schema" AS TEXT), CAST("c"."table_name" AS TEXT)) AS REGCLASS) AS OID), "c"."ordinal_position") AS "field-comment", (("column_default" IS NULL) OR (LOWER("column_default") = 'null')) AND ("is_nullable" = 'NO') AND NOT ((("column_default" IS NOT NULL) AND ("column_default" LIKE '%nextval(%')) OR ("is_identity" <> 'NO')) AS "database-required", "column_default" AS "database-default", (("column_default" IS NOT NULL) AND ("column_default" LIKE '%nextval(%')) OR ("is_identity" <> 'NO') AS "database-is-auto-increment", "is_generated" = $1 AS "database-is-generated", "is_nullable" = 'YES' AS "database-is-nullable" FROM "information_schema"."columns" AS "c" LEFT JOIN (SELECT "tc"."table_schema", "tc"."table_name", "kc"."column_name" FROM "information_schema"."table_constraints" AS "tc" INNER JOIN "information_schema"."key_column_usage" AS "kc" ON ("tc"."constraint_name" = "kc"."constraint_name") AND ("tc"."table_schema" = "kc"."table_schema") AND ("tc"."table_name" = "kc"."table_name") WHERE "tc"."constraint_type" = 'PRIMARY KEY') AS "pk" ON ("c"."table_schema" = "pk"."table_schema") AND ("c"."table_name" = "pk"."table_name") AND ("c"."column_name" = "pk"."column_name") WHERE c.table_schema !~ '^information_schema|catalog_history|pg_' AND ("c"."table_schema" IN ($2)) UNION ALL SELECT "pa"."attname" AS "name", CASE WHEN "ptn"."nspname" IN ('public', 'pg_catalog') THEN FORMAT('%s', "pt"."typname") ELSE FORMAT('"%s"."%s"', "ptn"."nspname", "pt"."typname") END AS "database-type", "pa"."attnum" - 1 AS "database-position", "pn"."nspname" AS "table-schema", "pc"."relname" AS "table-name", FALSE AS "pk?", NULL AS "field-comment", FALSE AS "database-required", NULL AS "database-default", FALSE AS "database-is-auto-increment", FALSE AS "database-is-generated", FALSE AS "database-is-nullable" FROM "pg_catalog"."pg_class" AS "pc" INNER JOIN "pg_catalog"."pg_namespace" AS "pn" ON "pn"."oid" = "pc"."relnamespace" INNER JOIN "pg_catalog"."pg_attribute" AS "pa" ON "pa"."attrelid" = "pc"."oid" INNER JOIN "pg_catalog"."pg_type" AS "pt" ON "pt"."oid" = "pa"."atttypid" INNER JOIN "pg_catalog"."pg_namespace" AS "ptn" ON "ptn"."oid" = "pt"."typnamespace" WHERE ("pc"."relkind" = 'm') AND ("pa"."attnum" >= 1) AND ("pn"."nspname" IN ($3)) ORDER BY "table-schema" ASC, "table-name" ASC, "database-position" ASC;

-- ============================================================
-- #13 first seen at 2026-06-12 12:40:59.419
-- ============================================================
SELECT * FROM (SELECT current_database() AS current_database, n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull  OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype  FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')  WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped  AND n.nspname LIKE $1 AND c.relname LIKE $2) c WHERE true  ORDER BY nspname,c.relname,attnum;

-- ============================================================
-- #14 first seen at 2026-06-12 12:40:59.427
-- ============================================================
SELECT        result.TABLE_CAT AS "TABLE_CAT",        result.TABLE_SCHEM AS "TABLE_SCHEM",        result.TABLE_NAME AS "TABLE_NAME",        result.COLUMN_NAME AS "COLUMN_NAME",        result.KEY_SEQ AS "KEY_SEQ",        result.PK_NAME AS "PK_NAME"FROM      (SELECT current_database() AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,   information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM, i.indnkeyatts as KEY_COUNT FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND n.nspname = $1 AND ct.relname = $2 AND i.indisprimary  ) result where  result.A_ATTNUM = (result.KEYS).x AND result.KEY_SEQ <= KEY_COUNT  ORDER BY result.table_name, result.pk_name, result.key_seq;

-- ============================================================
-- #15 first seen at 2026-06-12 12:40:59.537
-- ============================================================
SELECT "fk_ns"."nspname" AS "fk-table-schema", "fk_table"."relname" AS "fk-table-name", "fk_column"."attname" AS "fk-column-name", "pk_ns"."nspname" AS "pk-table-schema", "pk_table"."relname" AS "pk-table-name", "pk_column"."attname" AS "pk-column-name" FROM "pg_constraint" AS "c" INNER JOIN "pg_class" AS "fk_table" ON "c"."conrelid" = "fk_table"."oid" INNER JOIN "pg_namespace" AS "fk_ns" ON "c"."connamespace" = "fk_ns"."oid" INNER JOIN "pg_attribute" AS "fk_column" ON "c"."conrelid" = "fk_column"."attrelid" INNER JOIN "pg_class" AS "pk_table" ON "c"."confrelid" = "pk_table"."oid" INNER JOIN "pg_namespace" AS "pk_ns" ON "pk_table"."relnamespace" = "pk_ns"."oid" INNER JOIN "pg_attribute" AS "pk_column" ON "c"."confrelid" = "pk_column"."attrelid" WHERE fk_ns.nspname !~ '^information_schema|catalog_history|pg_' AND ("c"."contype" = 'f'::char) AND ("fk_column"."attnum" = ANY(c.conkey)) AND ("pk_column"."attnum" = ANY(c.confkey)) AND ("fk_ns"."nspname" IN ($1)) ORDER BY "fk-table-schema" ASC, "fk-table-name" ASC;

-- ============================================================
-- #16 first seen at 2026-06-12 12:40:59.722
-- ============================================================
-- Metabase
	SELECT SUBSTRING("public"."customers"."name", 1, 1234) AS "substring4476", SUBSTRING("public"."customers"."email", 1, 1234) AS "substring4477" FROM "public"."customers" LIMIT 10000;

-- ============================================================
-- #17 first seen at 2026-06-12 12:40:59.802
-- ============================================================
-- Metabase
	SELECT "public"."orders"."customer_id" AS "customer_id", "public"."orders"."total" AS "total", "public"."orders"."created_at" AS "created_at" FROM "public"."orders" LIMIT 10000;

-- ============================================================
-- #18 first seen at 2026-06-12 12:40:59.808
-- ============================================================
SELECT c.oid, a.attnum, a.attname, c.relname, n.nspname, a.attnotnull OR (t.typtype = 'd' AND t.typnotnull), a.attidentity != '' OR pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval(%' FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON (c.relnamespace = n.oid) JOIN pg_catalog.pg_attribute a ON (c.oid = a.attrelid) JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_catalog.pg_attrdef d ON (d.adrelid = a.attrelid AND d.adnum = a.attnum) JOIN (SELECT 16384 AS oid , 2 AS attnum UNION ALL SELECT 16384, 3 UNION ALL SELECT 16384, 4) vals ON (c.oid = vals.oid AND a.attnum = vals.attnum) where c.oid in (16384);

-- ============================================================
-- #19 first seen at 2026-06-12 12:46:46.435
-- ============================================================
SELECT '=== GUI QUESTION ===' AS marker;

-- ============================================================
-- #20 first seen at 2026-06-12 12:46:47.634
-- ============================================================
-- Metabase:: userID: 1 queryType: MBQL queryHash: a54de39466cbf3e82ff09c5a9a2bccf1f922bf9fd59b2717c1ad5e09a1c0bfbc
	SELECT COUNT(*) AS "count" FROM "public"."orders";


-- ============================================================
-- Observed bound parameter values during sync
-- ============================================================
-- #9 (get-tables 4-way JOIN):
--   $1 = '0', $2 = '0', $3 = '^pg_', $4 = 'information_schema', $5 = 'public'
-- #12 (information_schema.columns describe-fields):
--   $1 = 'ALWAYS', $2 = 'public', $3 = 'public'
-- #14 (pgjdbc getPrimaryKeys), one call per table:
--   $1 = 'public', $2 = 'customers'
--   $1 = 'public', $2 = 'orders'
-- #15 (FK introspection):
--   $1 = 'public'
