// Visualize a weekly Count of Parking Tickets in Los Angeles


import org.apache.spark.sql.functions._


import org.apache.spark.sql.expressions.Window


val weekly_df = grouped_dates.groupBy(window($"Issue Date","7 days") as "date")//Creating timeframe of 7 days using window
                             .sum() // sum all the violation counts
                             .select($"date.start" as "week_start",$"date.end" as "week_end",$"sum(count)" as "Total Violations",weekofyear($"date.start") as "week",year($"date.start") as "year")//select window start and window end dates and sum


display(weekly_df)
