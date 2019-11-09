library('RPostgreSQL')
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nc",host="host.docker.internal",port=5432,user="postgres",password="notverysecure", )

query <- sprintf("SELECT table_name FROM information_schema.tables limit 5") 
data <- dbGetQuery(con, query)
print(data)

query <-sprintf("select county_desc,precinct_desc,voter_party_code,count(*) from absentee where ballot_rtn_status='ACCEPTED' GROUP BY county_desc,precinct_desc,voter_party_code order by county_desc,precinct_desc,voter_party_code")
by_precinct <- dbGetQuery(con, query)

query <-sprintf("select county_desc,voter_party_code,count(*) from absentee where ballot_rtn_status='ACCEPTED' GROUP BY county_desc,voter_party_code order by county_desc,voter_party_code")
by_county <-dbGetQuery(con,query)

dbDisconnect(con)