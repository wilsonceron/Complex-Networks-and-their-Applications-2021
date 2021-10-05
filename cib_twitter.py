#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-**-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* 

											"cib.py"
											*********
								coordinated inauthentic behavior
								********************************

		Developed by: Wilson  Ceron		e-mail: wilsonseron@gmail.com 		Date: 15/08/2021
								

-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-**-*-*-**-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-* 
'''

import csv
import re
import sys, os
import time
import pandas as pd

from datetime import date,datetime,timedelta
from collections import Counter

import argparse
from argparse import RawTextHelpFormatter

from Common.classes.pre_processor import pre_processor


import warnings
warnings.filterwarnings("ignore")

n_parel_tweets	=	3	
interval		=	30

def find_cib(df, tweet):

	cib			=	-1
	new_cib		=	0
	new_cut		=	[]
	start_date	=	tweet['created_at']
	end_date	=	tweet['created_at']+timedelta(seconds=interval)
	old_date	=	end_date
	tweet_text	=	set(pre_processor(tweet['text']	, stop_words=True, keep_hashtags=True, keep_mentions=False,language = "pt"))
	
	while True:

		mask 		= (df['created_at'] >=start_date) & (df['created_at'] <= end_date)
		df_cut		=	df.loc[mask]

		if len(df_cut.index) <=1:
			df = df.drop(df_cut[mask].index)
			break

		for index,row in df_cut.iterrows(): 
			row_text	=	set(pre_processor(row['text']	, stop_words=True, keep_hashtags=True, keep_mentions=False,language = "pt"))
			if 	not	(
						(	(tweet_text				is row_text				)	and	(	len(row_text) 				> 0	)	) 
					or	(	(tweet['url']			is row['url'] 			)	and (	len(row['url']) 			> 0	)	) 
					or	(	(tweet['expanded_url']	is row['expanded_url']	)	and (	len(row['expanded_url'])	> 0	)	)	 
					):
				df_cut	=	df_cut.drop(index)

		if len(df_cut.index) <=1:
			break

		new_cib	=	len(df_cut.index)
		if cib < new_cib:
			old_date	=	end_date
			end_date	=	max(df_cut['created_at'])+timedelta(seconds=interval)
			cib			=	new_cib	
			new_cut		=	df_cut.copy()
		else:
			df_cut		=	new_cut
			df 			=	df.drop(df_cut[mask].index)	
			break

	return 	df,df_cut


def create_graphs(df, cib_tweets, cib_path):

	graph_path		=	cib_path+"/graph_files"
	os.makedirs(graph_path, exist_ok = True)

	all_users	=	[]
	edges_list	=	[]
	for tweet_id in cib_tweets:
		for i in tweet_id:
			source		=	 df.loc[df['tweet_id']==i]['user_id'].iloc[0]
			all_users.append(source)
			for j in tweet_id:
				target	=	df.loc[df['tweet_id']==j]['user_id'].iloc[0]
				source	=	str(source)	
				target	=	str(target)		
				edges_list.append((source,target))

	edges_dict		=	dict(Counter(edges_list))	


	f_echo	=	open(graph_path+"/echo_chambers_edges.csv","+w")	
	f_echo.write("source,target,weight\n")
	for k,v in edges_dict.items():
		f_echo.write(	str(k[0])	+","+	str(k[1])	+","+	str(v)	+"\n"	)	
	f_echo.close()

	
	f_echo		=	open(graph_path+"/echo_chambers_nodes.csv","+w")
	f_echo.write("id,label,verified\n")
	
	all_users	=	list(set(all_users))
	for user_id in all_users:

		label		=	'\"'	+			df.loc[df['user_id']== user_id]['username'].iloc[0]		+	'\"'
		verified	=	'\"'	+	str(	df.loc[df['user_id']== user_id]['verified'].iloc[0]	)	+	'\"'
		user_id		=	'\"'	+	str(						user_id							)	+	'\"'		

		f_echo.write(	str(user_id)	+","+	label+	","+	verified	+"\n")


	f_echo.close()



def main(argv):

	inputfile			=	"./input/antidemocratic_2020-01-01_2021-04-30.csv"
	df					=	pd.read_csv(inputfile)
	df					=	df.fillna('')
	df['created_at']	=	pd.to_datetime(df.created_at, format="%Y-%m-%dT%H:%M:%S.%f", errors='ignore')

	mask 	=	(df['created_at'] >=datetime(2014, 1, 1)) & (df['created_at'] <= datetime(2021, 8, 31))
	df		=	df.loc[mask]
	df		=	df.sort_values('created_at',ascending=True)

	u 		=	df['user_id'].value_counts()
	df		=	df[df['user_id'].isin(u[u>100].index)]	


	df.text			=	df.text.astype(str)
	df.url			=	df.url.astype(str)
	df.expanded_url	=	df.expanded_url.astype(str)


	df_tweets			=	df.copy()


	inputfile			=	inputfile[:-4]
	f					=	open(inputfile+"_cib_results.csv","+w")
	f.write("Fake_index,N_tweets,N_cib,n_tweets_cib\n")	

	cib_total			=	0
	cib_total_tweets	=	0
	n_tweets			=	0
	df					=	df.sort_values('created_at',ascending=True)
	dfs					=	[]
	i					=	1
	tweets_id			=	[]
	cut_ntweets			=	0
	n_tweets			=	len(df.index)
	cib					=	False
	print("Processing dataset (no retweets) with  " +str(len(df.index))+ " tweets"  )

	count	=	0
	size	=	len(df.index)

	print("Looking for coordinated inauthentic behavior....")	
	for index,tweet in df.iterrows():
		if not count%100:
			p	=	(1.*count/size)*100	
			print("\t"+str(round(p,2))+" % finished")	
		count	=	count	+	1
		
		df,df_cut	=	find_cib(df, tweet)
		if len(df_cut.index) >= n_parel_tweets:
			cib				=	True
			cib_path		=	"./"+inputfile+"_cib"
			os.makedirs(cib_path, exist_ok = True)
			df_cut['cib']	=	i
			cut_ntweets		=	cut_ntweets	+	 len(df_cut.index)
			dfs.append(df_cut)	
			i 				=	i	+	1
			tweets_id.append( list(df_cut['tweet_id'])) 
	if(cib):
		cib_total			=	cib_total		+	(i-1)
		cib_total_tweets	=	cib_total_tweets	+	cut_ntweets	
		f.write(inputfile+","+str(n_tweets)+","+str(cib_total)+","+str(cib_total_tweets)+"\n")	
		
		df_cut			=	pd.concat(dfs, join='outer', axis=0)	
		df_cut.to_csv(cib_path+"/"+inputfile+".csv",quoting=csv.QUOTE_NONNUMERIC,index=False)			
		print("Cib Total: " 		+str(cib_total))
		print("cib_total_tweets: " 	+str(cib_total_tweets))
	
		create_graphs(df_tweets, tweets_id, cib_path)

	f.close()
if __name__ ==  "__main__":
   main(sys.argv[1:])
