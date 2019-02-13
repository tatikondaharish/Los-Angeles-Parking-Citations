// Read the file from s3 with schema

val df = spark.read.format("csv").
              option("inferSchema",true).
              option("header",true).
              load("/mnt/parking_citation/parking_citation/parking-citations.csv")


//removing nulls from the dataset
val without_nulls_df = df.na.drop($"Issue Date")

import org.apache.spark.sql.functions._

val df_formatted = df.withColumn("Issue Date",to_date($"Issue Date"))


display(df_formatted.orderBy($"Issue Date".desc))//Convert Issue Date to Date format


// Group dataset using "Issue date"  and "Violation code" then counting them
val grouped_dates = df_formatted.groupBy($"Issue Date",$"Violation code").count().orderBy($"Issue Date".desc)

// COMMAND ----------

display(grouped_dates)


//Add dataframe argument to the method
//Violations for a given week
def violations_of_week (start : String, grouped_dates : DataFrame) = {
  grouped_dates.filter(($"Issue Date").between(to_date(lit(start)),date_add(to_date(lit(start)),7))) //Filter the dataset based on given dates
  .groupBy($"Violation code").sum().orderBy($"sum(count)".desc as "Total Violations") // Then apply group by on "Violation code" and sum the values of count and order them descending 
}

// COMMAND ----------

display(violations_of_week("2019-01-16").limit(5))


//  Visualize a trend of parking citations for a given week


display(violations_of_week("2019-01-16"))

