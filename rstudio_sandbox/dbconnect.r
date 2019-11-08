library('RPostgreSQL')
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nc",host="host.docker.internal",port=5432,user="postgres",password="notverysecure", )

query <- sprintf("SELECT table_name FROM information_schema.tables limit 5") 
data <- dbGetQuery(con, query)
print(data)


query <- sprintf("SELECT count(*) FROM results_pct") 
data <- dbGetQuery(con, query)
print(data)

dbDisconnect(con)