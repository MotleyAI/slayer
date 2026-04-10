# Time is the Hardest Dimension

A key property of a **well-designed semantic layer DSL** is being **expressive enough** to support all the **“natural”** queries and transforms, while remaining **as simple as possible**. 

In this post I’ll discuss what I consider to be natural query operations specifically relating to time, and how they’re implemented in SLayer.

First of all, consider **time dimensions**. They are different from most other dimensions in two important respects: firstly, a time column is **naturally ordered**; secondly, to usefully aggregate by it, you need to supply an additional parameter, the **aggregation granularity:** do you want **daily, weekly, monthly etc buckets**? 

From this we can already see some natural semantics that are special to queries with a time dimension. 

The first one is a **time-specific aggregation, “last”**. This works much like max or min, but orders by a time column instead of by the value itself. 

The next one is the **time shift: “same values, but for last year”**. This must be implemented as a sub-query that shifts the values of the time dimension by the desired amount before applying the granularity (after all, the shift value must not be an integer multiple of the granularity used, for example it’s perfectly valid to have a 3-day time shift while querying with weekly granularity). So we’re talking about a mildly intricate subquery to implement a semantically simple concept - clear usecase for a semantic layer, **implemented in SLayer as a time_shift transform**. 

You may think that at least in the case where the shift is an integer multiple of the granularity (for example, “previous week” for a weekly totals query) it could be easier to just query first, and then look up the respective value from the previous period in the query results. Sometimes that is indeed the case, and that is what ‘lag’ and ‘lead’ functions do. But the apparent simplicity and possible performance gain of that solution have a cost: the first period returned by the query doesn’t have a “previous” period, so the number of valid rows is reduced (eg 11 “previous” values for a one-year monthly query). It’s up to you to decide if that’s acceptable in your specific situation.

So now in the context of a query with a known time dimension and known granularity we have **a natural “previous” concept**. This means we also have a natural **“change”** concept, which in SLayer we can express as a **simple query-time change(x) transform** that needs no additional arguments, and just returns the change of its argument from one period (defined by the time dimension and granularity of the query) to the next one. 

Here we see the power of dynamic formulas - compare that e.g. to the Cube.js syntax that would require us to define the time-shifted version as a separate pre-defined measure in a cube, that hard-wires the time dimension and shift amount, and then yet another pre-defined measure that uses that to calculate the “change” quantity we want (now have fun pre-defining these measures for all needed time dimension/granularity combinations, and once you’ve done so have even more fun squishing the resulting cube model into agent context). 

The final time-related transform currently available in SLayer is **last()**. This is different from the previous transforms because taking the last value (for every combination of other dimensions in the query) is guaranteed to change the cardinality by **collapsing the time dimension**, so again needs a subquery that can be joined to the rest of the query by the values of the remaining dimensions. 

Again, **this needs no additional arguments because we already know the time dimension and granularity from the query**. So if you want to only show e.g. revenue by customer only for the customers whose MoM spending has decreased in the last month, you can just group by customer_name and filter by last(change(revenue:sum)) < 0.

Each of these concepts is very **simple and natural at the semantic level**, but requires **nontrivial SQL semantics** (typically via a subquery) to implement. **SLayer offloads the effort of creating these from the agent, saving tokens, reducing error risk, and freeing the agent’s cognitive capacity and context for tasks you care about.**

Here is a complete example query for “monthly revenue for the last year by region, compared to previous year, for regions whose MoM number of website visits is down”:

```json
{
  "source_model": "my_model",
  "fields": [
    "revenue:sum",
    "time_shift(revenue:sum, -1, 'year')"
  ],
  "dimensions": ["region"],
  "time_dimensions": [{
    "dimension": "created_at",
    "granularity": "month",
    "date_range": ["2025-01-01", "2025-12-31"]
  }],
  "filters": ["last(change(website_visits:sum)) < 0"]
}
```

What would it take to define this in the semantic layer you’re currently using?

---

See the [companion notebook](time_nb.ipynb) for runnable code demonstrating all time-related transforms.