drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nc",host="host.docker.internal",port=5432,user="postgres",password="notverysecure", )

query <- sprintf("SELECT table_name FROM information_schema.tables limit 5") 
data <- dbGetQuery(con, query)

query <- sprintf("INSERT INTO results (county) VALUES ('Philadelphia') ")
place_holder <- dbSendQuery(con,query)

query <- sprintf("SELECT * FROM results") 
data <- dbGetQuery(con, query)
print(data)

dbDisconnect(con)