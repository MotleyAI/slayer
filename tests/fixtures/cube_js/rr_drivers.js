// Anonymized, condensed replica of the RrDrivers return-rate-decomposition
// cube (DEV-1730). SQL ported to DuckDB (TIMESTAMP for TIMESTAMP_NTZ,
// `- INTERVAL 1 YEAR` for DATEADD, generic analytics.* tables). Kept only the
// constructs the importer must represent: template-literal cube name; a TY/LY
// UNION ALL CTE chain; a scalar-position value-arrow FILTER_PARAMS with a
// ::TIMESTAMP cast; a required range-arrow (concat form) incl. the LY-shifted
// variant; optional categorical string-form pushdowns; WHERE 1=1; SQL comments;
// quoted identifiers; a composite `||` PK; a renamed dim; a required time dim
// with meta.required; a measure literally named `count`; `max` measures with
// `format: percent`; a calc measure `${share_ty} - ${share_ly}`.
cube(`RrDrivers`, {
    description: `Return-rate decomposition. Requires a fulfillment_date filter.`,

    sql: `
      -- 1. FILTER & TAG (TY vs LY), filters pushed down via FILTER_PARAMS
      WITH filtered AS (
          SELECT
              it.category                       AS category,
              fl.market                         AS country,
              it.brand                          AS brand,
              fl.quantity_fulfilled             AS qf,
              fl.quantity_returned              AS qr,
              'TY'                              AS period,
              ${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => from)}::TIMESTAMP AS ty_start_date
          FROM analytics.fact_lines fl
          LEFT JOIN analytics.dim_items it ON fl.product_key = it.product_key
          WHERE 1 = 1
              AND ${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => 'fl."FULFILLMENT_DATE" >= ' + from + ' AND fl."FULFILLMENT_DATE" <= ' + to)}
              AND ${FILTER_PARAMS.RrDrivers.category.filter('it."CATEGORY"')}
              AND ${FILTER_PARAMS.RrDrivers.brand.filter('it."BRAND"')}
              AND ${FILTER_PARAMS.RrDrivers.market.filter('fl."MARKET"')}

          UNION ALL

          SELECT
              it.category                       AS category,
              fl.market                         AS country,
              it.brand                          AS brand,
              fl.quantity_fulfilled             AS qf,
              -- numerator-only late-arrival filter: exclude returns received in
              -- the last 365 days so LY matches TY's maturity window.
              CASE
                  WHEN fl.date_returned IS NULL OR fl.date_returned < CURRENT_DATE - INTERVAL 1 YEAR
                  THEN fl.quantity_returned
                  ELSE 0
              END                               AS qr,
              'LY'                              AS period,
              ${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => from)}::TIMESTAMP AS ty_start_date
          FROM analytics.fact_lines fl
          LEFT JOIN analytics.dim_items it ON fl.product_key = it.product_key
          WHERE 1 = 1
              AND ${FILTER_PARAMS.RrDrivers.fulfillment_date.filter((from, to) => 'fl."FULFILLMENT_DATE" >= CAST(' + from + ' AS DATE) - INTERVAL 1 YEAR AND fl."FULFILLMENT_DATE" <= CAST(' + to + ' AS DATE) - INTERVAL 1 YEAR')}
              AND ${FILTER_PARAMS.RrDrivers.category.filter('it."CATEGORY"')}
              AND ${FILTER_PARAMS.RrDrivers.brand.filter('it."BRAND"')}
              AND ${FILTER_PARAMS.RrDrivers.market.filter('fl."MARKET"')}
      ),

      -- 2. AGGREGATE to category x country x period
      agg AS (
          SELECT category, country, period,
                 SUM(qf) AS qty_fulfilled,
                 SUM(qr) AS qty_returned,
                 MIN(brand) AS brand,
                 MIN(ty_start_date) AS ty_start_date
          FROM filtered
          GROUP BY category, country, period
      ),

      -- 3. PIVOT to wide (one row per category x country)
      wide AS (
          SELECT
              COALESCE(ty.category, ly.category) AS category,
              COALESCE(ty.country, ly.country)   AS country,
              COALESCE(ty.brand, ly.brand)       AS brand,
              COALESCE(ty.qty_fulfilled, 0)      AS qf_ty,
              COALESCE(ly.qty_fulfilled, 0)      AS qf_ly,
              COALESCE(ty.qty_returned, 0)       AS qr_ty,
              COALESCE(ly.qty_returned, 0)       AS qr_ly,
              COALESCE(ty.ty_start_date, ly.ty_start_date) AS ty_start_date
          FROM      (SELECT * FROM agg WHERE period = 'TY') ty
          FULL JOIN (SELECT * FROM agg WHERE period = 'LY') ly
                 ON ty.category = ly.category AND ty.country = ly.country
      ),

      -- 4. FINAL OUTPUT: per-segment return rates + shares
      final_output AS (
          SELECT
              category || '|' || country AS row_key,
              category,
              country,
              country AS market,
              brand,
              qf_ty, qf_ly, qr_ty, qr_ly,
              CASE WHEN qf_ty > 0 THEN CAST(qr_ty AS DOUBLE) / qf_ty ELSE 0 END AS rr_ty,
              CASE WHEN qf_ly > 0 THEN CAST(qr_ly AS DOUBLE) / qf_ly ELSE 0 END AS rr_ly,
              CAST(qf_ty AS DOUBLE) / NULLIF((SELECT SUM(qf_ty) FROM wide), 0) AS share_ty,
              CAST(qf_ly AS DOUBLE) / NULLIF((SELECT SUM(qf_ly) FROM wide), 0) AS share_ly,
              ty_start_date
          FROM wide
      )
      SELECT * FROM final_output
    `,

    dimensions: {
        primary_key: {
            sql: `${CUBE}.category || '|' || ${CUBE}.country`,
            type: `string`,
            primaryKey: true,
            public: false,
        },
        category: {
            sql: `${CUBE}.category`,
            type: `string`,
            title: `Category`,
        },
        country: {
            sql: `${CUBE}.country`,
            type: `string`,
            title: `Country`,
        },
        market: {
            sql: `${CUBE}.market`,
            type: `string`,
            title: `Market`,
        },
        // renamed/aliased dim: exposes the same physical column under an alias
        shipping_country: {
            sql: `${CUBE}.country`,
            type: `string`,
            public: false,
        },
        brand: {
            sql: `${CUBE}.brand`,
            type: `string`,
            title: `Brand`,
        },
        fulfillment_date: {
            sql: `${CUBE}.ty_start_date`,
            type: `time`,
            title: `Fulfillment Date (TY Period)`,
            description: `Required filter. Sets the TY period; LY is the same window shifted back 1 year.`,
            meta: {
                required: true,
            },
        },
    },

    measures: {
        count: {
            type: `count`,
            title: `Driver Count`,
        },
        rr_ty: {
            sql: `${CUBE}.rr_ty`,
            type: `max`,
            title: `Return Rate TY`,
            format: `percent`,
            description: `Return rate in the TY period`,
        },
        rr_ly: {
            sql: `${CUBE}.rr_ly`,
            type: `max`,
            title: `Return Rate LY`,
            format: `percent`,
        },
        share_ty: {
            sql: `${CUBE}.share_ty`,
            type: `max`,
            title: `Share TY`,
            format: `percent`,
        },
        share_ly: {
            sql: `${CUBE}.share_ly`,
            type: `max`,
            title: `Share LY`,
            format: `percent`,
        },
        share_change: {
            sql: `${share_ty} - ${share_ly}`,
            type: `number`,
            title: `Share Change`,
            format: `percent`,
            description: `Change in share of total (TY - LY)`,
        },
        qty_ty: {
            sql: `${CUBE}.qf_ty`,
            type: `max`,
            title: `Units Fulfilled TY`,
        },
    },

    pre_aggregations: {},
});
