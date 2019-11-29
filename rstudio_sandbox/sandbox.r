wide_votes <- votes %>% dcast(contest_name + county ~ choice_party,sum)
votes_party_list <- unique(votes$choice_party)
wide_votes$total <- rowSums(wide_votes[,c(votes_party_list)])
wide_votes$DEM_pct <- with(wide_votes,DEM/total)

pivot <- function(df,cat_fld,num_fld){
  cf <- enquo(cat_fld)
  nf <- enquo(num_fld)
  out <- df %>% group_by(!! cf) %>% summarize(abs_votes=sum(!! nf))
  return(head(out))
  
}
pivot(votes,contest_name,abs_votes)