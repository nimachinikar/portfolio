#install.packages("tidyverse")
#install.packages("dplyr")
library('tidyverse')
library('dplyr')
library('ggplot2')

Soccer_BL_event <- read_csv('C:\\GitHub\\portfolio\\DataSet\\R\\Soccer\\events_bundesliga_2022-23.csv', col_names = TRUE 
#https://www.kaggle.com/datasets/mcarujo/european-football-season-202223
         #, col_names = c("x", "y", "z")
         #, na = "."
         # comment = "#",
)



#Bins
?max

df<-Soccer_BL_event %>% 
            mutate(period = cut(event_time, 
                                  breaks=c(0,45, max(Soccer_BL_event$event_time) ),
                                  labels = c("First", "Second"))) %>%
                select (-event_time)

#Different values for event_type
df %>% data.frame() %>%
      dplyr::distinct(event_type)


#Within Group Ranking
df<- df %>%                     
  group_by(match_id,team,event_type) %>%
  mutate(EventOccurenceByGame = row_number())

#Creating Binary column
df <-df %>% 
          mutate(HomeGame= case_when(
                    event_team=='home' ~ TRUE,
                    event_team=='away' ~ FALSE,
                    )) %>%
          select(-event_team)
          


# Adding Abreviation for each team (in another tbl atm)
teamAbv <- c('CGN','UNB','AUG', 'B04','BAY','DOR','SGE','BCS','TSG','BMG','MAI','RBL','SCF','S04','STU','BOC','WOB','SVW')	

#First Col comes from df to avoid mistake
dimTeam <- data.frame(df%>% data.frame()%>%distinct(team)%>% arrange(team), teamAbv)
#adding ID
dimTeam<- dimTeam %>%                     
  mutate(team_id = row_number())

#adding abv and id to df
df<- inner_join(df, dimTeam)

#Creating new column having the fixture home-away team
#step 1 Keeping only interesting columns
dfSubset<- df %>% data.frame() %>%
            distinct(match_id, HomeGame,team) %>%
            inner_join(dimTeam) %>%
            select(-team,-team_id)
            
#step 2 creating one home df and one away
dfSubsetHome<-filter(dfSubset,HomeGame==1)
dfSubsetAway<-filter(dfSubset,HomeGame==0)

#step 3joining home and away df
dfSubset<- inner_join(dfSubsetHome, dfSubsetAway, 
                       by = "match_id")

#(step 4) cleaning/ renaming
dfSubset<- dfSubset %>% select(match_id, teamAbv.x,teamAbv.y) %>%
                         rename(
                             HT = teamAbv.x,
                             AT = teamAbv.y)
#step 5 creating fixture  
dfSubset$fixtures<-paste(dfSubset$HT,dfSubset$AT,sep=" - ")

#step 6 dropping unnessary column before joining
dfSubset<- dfSubset %>% select(match_id, fixtures)

#step 7 joining to original df
df <- inner_join(df,dfSubset)

#(step 8) dropping dev df
rm(dfSubsetAway)
rm(dfSubsetHome)
rm(dfSubset)

#Reorganizing
df<- df %>% select(event_id,match_id,fixtures, team, teamAbv, team_id, HomeGame, event_type,action_player_1, action_player_2, Period, EventOccurenceByGame )

#DASHBOARD
#ResultByTeam
dfbyTeam <-  df %>% 
            data.frame() %>% 
            select(match_id,team, teamAbv, HomeGame, event_type, EventOccurenceByGame) %>% 
            group_by(match_id,team,teamAbv,event_type) %>% 
            summarise(OccurencePerGame =max(EventOccurenceByGame))

dfbyTeam <- dfbyTeam %>% group_by(team,teamAbv,event_type )%>% summarise(Occurence=sum(OccurencePerGame))

#Graph 1: seeing all the event at once  
ggplot(data=dfbyTeam, aes(y=Occurence, x=teamAbv, fill=event_type)) +
    geom_bar(stat="identity")+
  scale_fill_manual(breaks = c('Goal','Penalty','Missed penalty','Disallowed goal','Own goal', 'Yellow card','Second yellow card','Red card','Substitution'),
                    values=c('black',"green", 'purple' , 'lightgrey','lightblue','yellow','orange','red',"darkblue"))

#Graph 2: seeing subsets of events
ggplot(data=filter(dfbyTeam,event_type %in% c('Goal','Yellow card','Second yellow card','Red card')), aes(y=teamAbv, x=Occurence, fill=event_type)) +
  geom_bar(stat="identity", color="black", position=position_dodge())+
  theme_minimal()

#Graph 3: see one event
ggplot(data=filter(dfbyTeam,event_type =='Goal') , aes(y=reorder(teamAbv,(Occurence)), x=Occurence)) +
  geom_bar(stat="identity")+
    theme_minimal() +
  labs(y= "teamsName")

#TODO More Graph, more analysis