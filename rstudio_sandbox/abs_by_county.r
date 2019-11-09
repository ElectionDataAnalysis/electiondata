install.packages('reshape')
library('RPostgreSQL','reshape','dplyr')

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nc",host="host.docker.internal",port=5432,user="postgres",password="notverysecure", )

query <-sprintf("select county_desc,cong_dist_desc,voter_party_code,count(*) as abs_ballots from absentee where ballot_rtn_status='ACCEPTED' and ballot_req_type ='MAIL' GROUP BY county_desc,cong_dist_desc,voter_party_code order by county_desc,cong_dist_desc,voter_party_code")
ballots <- dbGetQuery(con,query)
ballots_by_dist_party <- ballots %>% group_by(cong_dist_desc,voter_party_code) %>% summarise(sum(abs_ballots,na.rm=TRUE))
ballots_by_dist <- ballots %>% group_by(cong_dist_desc)  %>% summarise(sum(abs_ballots,na.rm=TRUE))

query <- sprintf("select county, contest_name, choice_party, sum(absentee_by_mail) abs_votes from results_pct where contest_name like '%%US HOUSE%%' group by county, contest_name, choice_party order by county, choice_party")
votes <- dbGetQuery(con,query)
votes_by_dist_party <- votes %>% group_by(contest_name,choice_party) %>% summarise(sum(abs_votes,na.rm = TRUE))
votes_by_dist <- votes %>% group_by(contest_name) %>% summarise(sum(abs_votes,na.rm = TRUE))

dbDisconnect(con)
