drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="tmp",host="host.docker.internal",port=5432,user="postgres",password="notverysecure", )

query <- sprintf("SELECT table_name FROM information_schema.tables limit 5") 
data <- dbGetQuery(con, query)

dbDisconnect(con)