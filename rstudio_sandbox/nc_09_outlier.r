install.packages('reshape2','outliers')
library('RPostgreSQL')
library('reshape2')
library('dplyr')
library('lazyeval')
library('outliers')

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname="nc",host="host.docker.internal",port=5432,user="postgres",password="notverysecure", )

query <-sprintf("select county_desc,cong_dist_desc,voter_party_code,count(*) as abs_ballots from absentee where ballot_rtn_status='ACCEPTED' and ballot_req_type ='MAIL' GROUP BY county_desc,cong_dist_desc,voter_party_code order by county_desc,cong_dist_desc,voter_party_code")
ballots <- dbGetQuery(con,query)

query <- sprintf("select county, contest_name, choice_party, sum(absentee_by_mail) abs_votes from results_pct where contest_name like 'NC_USC%%' group by county, contest_name, choice_party order by county, choice_party")
votes <- dbGetQuery(con,query)

dbDisconnect(con)



wide_votes <- votes %>% dcast(contest_name + county ~ choice_party,sum)
votes_party_list <- unique(votes$choice_party)
wide_votes$total <- rowSums(wide_votes[,c(votes_party_list)])
wide_votes$DEM_pct <- with(wide_votes,DEM/total)
v09 <- wide_votes %>% filter(contest_name=="NC_USC_09_2018")

wide_ballots <- ballots %>% dcast(cong_dist_desc + county_desc ~ voter_party_code,sum)
ballots_party_list <- unique(ballots$voter_party_code)
wide_ballots$total <- rowSums(wide_ballots[,c(ballots_party_list)])
wide_ballots$DEM_pct <- with(wide_ballots,DEM/total)

ballots_and_votes <- full_join(wide_ballots,wide_votes, by = c("cong_dist_desc"="contest_name","county_desc"="county"))

plot(x=ballots_and_votes$DEM_pct.x,
                y=ballots_and_votes$DEM_pct.y,
                xlab="ballots",
                ylab="votes",
                main="DEM Pct by Ballots vs. by Votes",
                xlim=c(0,1),
                ylim=c(0,1))
# restrict to 9th district
bv09 <- ballots_and_votes %>% filter(cong_dist_desc == "NC_USC_09_2018")
outlier(bv09$DEM_pct.y)
