#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[5]:


class utils:
    def __init__(self, givenlist): # givenlist: a list of dictionaries
        self.givenlist = givenlist
        
    
    def combined_dict(self):  # creates a single dictionary after combining along common keys for the different dictionaries in givenlist
        givenlist = self.givenlist
        if all(isinstance(item, dict) for item in givenlist) == False:
             raise Exception("givenlist must be a list of dictionaries")
        all_dict = {}
        for event in givenlist: 
            for i in event: 
                if i in all_dict: 
                    all_dict[i].append(event[i]) 
                else: 
                    all_dict[i] = [event[i]]                
        return all_dict

    
    def creat_df(self): #creat_df uses the function combined_dict.
        combinedDict = self.combined_dict()
        output_df = pd.DataFrame(combinedDict) 
        return output_df
    


# In[7]:


class events(utils):
    
    event_df = None
    intermediate_df = False
    df_list = []
    current_query = None
    
    
    def __init__(self, givenlist, compname, tags_name, events_label):
        # compname: a string
        # tags_name: dataframe that maps tag ids to tag names (e.g. 1801 means accurate)
        # events_label: dataframe that maps event labels to event names (e.g. 8 means Pass)
        super().__init__(givenlist)
        self.givenlist = givenlist
        self.compname = compname
        self.tags_name = tags_name
        self.events_label = events_label

    def _df_with_tags(self):
        df = self.creat_df()
        tags_list = list(self.tags_name['Tag'])
        all_ind = []
        for tag in df['tags']:
            ind_row = [0] * len(tags_list)
            for i in range(len(tag)):
                ind =  tags_list.index(tag[i]["id"])
                ind_row[ind] = 1
            all_ind.append(ind_row)

        tags_df = pd.DataFrame(all_ind)
        tags_df.columns = list(self.tags_name['Label'])
        df_tags = pd.concat([df, tags_df], axis = 1)
        compnam_df = pd.DataFrame({"competition" :  [self.compname] * df_tags.shape[0]})
        df_tags = pd.concat([compnam_df, df_tags], axis = 1)
        return df_tags
    
    def process(self, teams_df, players_df, output = False):
        #teams_df: dataframe with team id and team name 
        #output: boolean; True will show the resulting dataframe
        event_df = self._df_with_tags()
        event_df = event_df.join(self.events_label, on = "eventId") #giving events name from events_label instead of coded labels for events
        event_df = event_df.join(teams_df, on = "teamId") #giving team name from teams_df instead of coded labels for teams
        event_df = event_df.join(players_df, on = "playerId") #giving team name from player_df instead of coded labels for players
        event_df = event_df.drop(['city', 'officialName', 'area', 'type'], axis = 1) #removing unnecessary columns
        self.event_df = event_df
        self.intermediate_df = True
        if output == True:
            return event_df
    
    def query_tag(self, tag, by = 'team', newcolnames = None, output = False): 
        #Will return summarized reulst by team or player based on the tag.
        #E.g. a tag query of 'accurate' will retrun sum of accurates by team or player 
        #irresepective of whether this is a pass or any other event.
        #tags: list of tags. E.g. ['Goal', 'acurate']
        #by = string. generally it will be 'team' or 'player'
        if self.intermediate_df == False:
            raise Exception('Need to run the process method before querying') 
        event_df = self.event_df
        subset = event_df[tag + [by]]
        gb = subset.groupby(by)
        gb_temp = gb[tag].sum()
        gb_temp.sort_index(ascending = True, inplace = True)
        if newcolnames != None:
            gb_temp.columns = newcolnames
        self.df_list.append(gb_temp)
        if output == True:
            return gb_temp
            
    def query_eventOrSub(self, querylist, by = 'team', output = False, level = 'event'):
        #Will return summarized reulst (total count to be exact) by team or player based on the event.
        #E.g. a events query of 'Pass' will retrun count of total passes by team or player.
        #events: name of one or more events or subevents in a list. E.g. ['Pass', 'Duel']
        #by = string. generally it will be 'team' or 'player'
        #level = string; could be 'event' or 'subevent'
        if self.intermediate_df == False:
            raise Exception('Need to run the process method before querying') 
        if level == 'event':
            colum_target = "eventName"
        elif level == 'subevent':
            colum_target = "subEventName"
        q_df = []
        for event in querylist:
            event_df = self.event_df
            filt = (event_df[colum_target] == event)
            gb = event_df[filt].groupby(by)
            gb_temp = pd.DataFrame(gb.agg('count')[colum_target])
            gb_temp.sort_index(ascending = True, inplace = True)
            gb_temp.columns = [f"total_{event}"]
            self.df_list.append(gb_temp)
            q_df.append(gb_temp)
        if output == True:
            return q_df
    
    
    def _adddf(self, listOfdf):
        result_df = listOfdf[0]
        added_df = []
        col_name = list(set(result_df.columns))
        for i in range(1, len(listOfdf)):
            result_df = result_df.merge(listOfdf[i], left_index=True, right_index=True)
               
        for col in col_name:
            temp = result_df.filter(like=col)
            temp2 = pd.DataFrame(temp.sum(axis = 1))
            temp2.columns = [col]
            added_df.append(temp2)
            fin_df = added_df[0]
            if len(added_df)>1:    
                for i in range(1, len(added_df)):
                    fin_df = fin_df.merge(added_df[i], left_index=True, right_index=True)
        return fin_df
            
        
    def _search_eventsub_tag(self, tags, events, colum_target, by = 'team'):                    
        df_items = []
        for i in range(len(events)):
            event_df = self.event_df
            filt = (event_df[colum_target] == events[i])
            event_df = event_df[filt] 
            event_df = event_df[tags + [by]]
            gb = event_df[filt].groupby(by)
            gb_temp = gb[tags].sum()
            gb_temp.sort_index(ascending = True, inplace = True)
            df_items.append(gb_temp)
        return df_items
            
    def _pre_query_eventsub_tag(self, tags, events = None, subevents = None, by = 'team'):     
        if subevents == None:
            colum_target = "eventName"
            return self._search_eventsub_tag(tags = tags, events = events, by = by, colum_target = colum_target) 
        else:
            events = subevents
            colum_target = "subEventName"
            return self._search_eventsub_tag(tags = tags, events = events, by = by, colum_target = colum_target)
                
     
    def _newname(self, dflist, tags, events, combine):
        if combine == True:
            comb_df = self._adddf(dflist)
            newnames = ['total'+'_'+col for col in comb_df.columns]
            comb_df.columns = newnames
            return comb_df
        else:
            for i in range(len(events)):
                newnames = [events[i]+'_'+col for col in dflist[i].columns]
                dflist[i].columns = newnames
            return dflist
  
    def _givename(self, dflist, tags, events, subevents, combine):
        if events is not None:
            return self._newname(dflist = dflist, tags = tags, events = events, combine = combine)
        else:
            events = subevents
            return self._newname(dflist = dflist, tags = tags, events = events, combine = combine)
    
    def query_eventsub_tag(self, tags, events = None, subevents = None, by = 'team', output = False, combine = False):
        #returns summarized result for single/multiple tags and event or subevents combination. 
        #events: name of one or more events in a list. E.g. ['Pass', 'Duel']
        #subevents: name of one or more subevents in a list. E.g. ['Cross', 'Free Kick']
        #tags: list of tags. E.g. ['Goal', 'acurate']
        #by = string. generally it will be 'team' or 'player'
        #output: boolean; True will show the resulting dataframe
        #combine: boolean; setting it True will sum the resulting dataframes. E.g. if interested in total goals from events 'Shot', 'Free Kick' 
        
        if self.intermediate_df == False:
            raise Exception('Need to run the process method before querying') 
        if int(events is None) + int(subevents is None) == 2:
            raise Exception('Can not have both events and subevents to be None')
        if self.current_query != by:
            if self.current_query is not None:
                raise Exception('Clear the query using refresh_query_list method. Make sure to save your previous query before doing that.')
        
        self.current_query = by
         
        list_comb_df = self._pre_query_eventsub_tag(tags, events = events, subevents = subevents, by = by)
        result_query = self._givename(dflist = list_comb_df, tags = tags, events = events, subevents = subevents, combine = combine)
         
        if isinstance(result_query, list):
            for item in result_query:
                self.df_list.append(item)
        else:
            self.df_list.append(result_query)

        if output == True:
            return result_query
                   
    def concat_df(self): #can be used when givenlist is a list of dataframes.
        
        if len(self.df_list) == 0:
            raise Exception('There is no dataframe in the list')
        df_list = self.df_list 
        result_df = df_list[0]
        
        if len(df_list)>1:   
            for i in range(1, len(df_list)):
                result_df = result_df.merge(df_list[i], left_index=True, right_index=True)

        return result_df

    def refresh_query_list(self):
        #to empty the dataframe list resulted from previous queries
        self.df_list.clear()
        self.current_query = None


# In[ ]:




