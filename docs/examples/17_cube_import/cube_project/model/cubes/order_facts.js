// A JavaScript sql-mode Cube with FILTER_PARAMS pushdowns. The importer turns
// each `FILTER_PARAMS.<cube>.<member>.filter('col')` into a SLayer `{var}`
// substitution over a `col IN ({var})` template:
//   * `category` carries `meta.required`, so its pushdown is REQUIRED — a bare
//     `p.category IN ({category})`; omitting it raises.
//   * `region` has no `meta.required`, so its pushdown is OPTIONAL — wrapped in
//     an optional block `{? c.region IN ({region}) ?}` that collapses to
//     `(1=1)` when the caller omits it.
// Both are string-form, so the importer marks them `list_valued` and the engine
// coerces a bare scalar to a one-element list before substituting.
cube(`order_facts`, {
    sql: `
      SELECT o.order_id, o.amount, o.status, c.region, p.category
      FROM orders o
      LEFT JOIN customers c ON o.customer_id = c.customer_id
      LEFT JOIN products  p ON o.product_id  = p.product_id
      WHERE 1 = 1
          AND ${FILTER_PARAMS.order_facts.region.filter('c.region')}
          AND ${FILTER_PARAMS.order_facts.category.filter('p.category')}
    `,

    dimensions: {
        order_id: {
            sql: `${CUBE}.order_id`,
            type: `number`,
            primaryKey: true,
            public: false,
        },
        status: {
            sql: `${CUBE}.status`,
            type: `string`,
        },
        region: {
            sql: `${CUBE}.region`,
            type: `string`,
            description: `Optional pushdown — filters when supplied, no-op when omitted.`,
        },
        category: {
            sql: `${CUBE}.category`,
            type: `string`,
            description: `Required pushdown — must be supplied or the query raises.`,
            meta: {
                required: true,
            },
        },
    },

    measures: {
        count: {
            type: `count`,
        },
        total_amount: {
            sql: `${CUBE}.amount`,
            type: `sum`,
            title: `Total Amount`,
        },
    },
});
