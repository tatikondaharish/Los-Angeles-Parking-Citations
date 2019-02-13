// Yearly citation trend by Car type


//add a column year which is derived from issue date and 
// group data by year and make and then count 
display(df.select('*,year($"Issue Date") as "year").groupBy($"year",$"Make").count())
