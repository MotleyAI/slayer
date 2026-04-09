# Make it dynamic!

A key distinction between SLayer and most other semantic layers an BI tools is its **dynamic nature**. This means it’s harder for it to pre-compute aggregates, but **easier for its users to formulate the queries they need**, even if those exact queries weren’t foreseen by the existing model definitions. This is a conscious choice, as our intended users are agents and humans formulating ad hoc queries, rather than dashboards showing the same thing to many people day in, day out. 

What do we mean by “dynamic”? 

First and simplest, this means **query-time transforms**, as described in the [time transforms](../04_time/index.md) post. If you want to time-shift a measure, compute a ratio, etc, you can define the corresponding expressions right in your query, no model changes needed. 

Second, this means **extending your models on the fly**: when constructing a query, you have to choose the root model the query will apply to (and have access to its [joined models](../05_joins/index.md)); but in addition to specifying a model name for this, you can also [specify additional measures, dimensions, filters, and joins](../05_joins/index.md#dynamic-joins-modelextension), that will be appended to the model (or to be precise, to the copy of the model used for this query) before evaluating the query.

A common usecase for that is **defining buckets**, for example if the query underlying the original model contains a floating-point number, and you want to group by whether that number is positive, you could add, right inside the query, a dimension to the model with an if-then-else expression that does just that. 

While usecases for query-time adding of dimensions and measures to models is easy to see, you may wonder what the rationale for adding joins on the fly may be. If there is a relationship between this model and another, would it not make sense to put that into the original saved model definition in the first place? 

The answer is that **queries can also be used as models,** and combined with [dynamic joins](../05_joins/index.md#dynamic-joins-modelextension) enable a **concise, natural, and powerful representation of general [multistage queries](../06_multistage_queries/index.md)**.