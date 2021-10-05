#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unicodedata
import nltk
import re
import codecs
import csv
import sys, getopt
import preprocessor as p
from nltk.stem import RSLPStemmer
from nltk.corpus import stopwords
from collections import Counter
import pycurl
import argparse
from argparse import RawTextHelpFormatter

#https://pypi.org/project/tweet-preprocessor/
#https://www.linkedin.com/pulse/classifica%C3%A7%C3%A3o-de-textos-em-python-luiz-felipe-araujo-nunes


#p.OPT.SMILEY
p.set_options( p.OPT.EMOJI, p.OPT.MENTION, p.OPT.HASHTAG, p.OPT.RESERVED,p.OPT.NUMBER)

def remove_url(tweet):
	URL_PATTERN =	re.compile(r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?]))')
	return URL_PATTERN.sub(r'', tweet)

def remove_accents(tweet): 
	tweet	=	unicodedata.normalize('NFKD', tweet).encode('ASCII', 'ignore').decode()
	return tweet

def strip_underscore(s):
	punctuation	=	'_'
	t	=	''.join(c for c in s if c not in punctuation)
	return t


def strip_punctuation(s):
	punctuation	=	'!"#$&\'()*+,-./:;<=>?%@[\\]^`{|}~…'
	t			=	''.join(c for c in s if c not in punctuation)
	return t

def hasNumbers(inputString):
	return any(char.isdigit() for char in inputString)


def stem(tweet):
	t	= tweet.split()	
	if t:
		stemmer		=	RSLPStemmer()
		for item in t:
			item=stemmer.stem(item)
		return ' '.join(t)
	return ''

def replace_expressions(tweet, language):

	with open('./Common/data/PreProcessor/Universal.csv', newline='') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for row in spamreader:
			tweet = re.sub(	row[0]	,	row[1]	,	tweet	)


	with open('./Common/data/PreProcessor/Names.csv', newline='') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for row in spamreader:
			tweet = re.sub(	row[0]	,	row[1]	,	tweet	)

	if language=="pt":

		with open('./Common/data/PreProcessor/PT.csv', newline='') as csvfile:
			spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in spamreader:
				tweet = re.sub(	row[0]	,	row[1]	,	tweet	)	


	if language=="es":

		with open('./Common/data/PreProcessor/ES.csv', newline='') as csvfile:
			spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in spamreader:
				tweet = re.sub(	row[0]	,	row[1]	,	tweet	)	

	if language=="en":

		with open('./Common/data/PreProcessor/EN.csv', newline='') as csvfile:
			spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
			for row in spamreader:
				tweet = re.sub(	row[0]	,	row[1]	,	tweet	)	

	
	return 	tweet

def remove_stopWords(tweet,language):

	t	=	tweet.split()
	if t:
		if language=="pt":

			stop			=	stopwords.words('portuguese')
			stop			=	[ x.upper() for x in stop]
			f				=	open("./Common/data/PreProcessor/custom_stopwords_PT.txt")

		if language=="es":

			stop			=	stopwords.words('spanish')
			stop			=	[ x.upper() for x in stop]
			f				=	open("./Common/data/PreProcessor/custom_stopwords_ES.txt")
			
		if language=="en":

			stop			=	stopwords.words('english')
			stop			=	[ x.upper() for x in stop]
			f				=	open("./Common/data/PreProcessor/custom_stopwords_EN.txt")
			

		languageStopWords	=	f.read().splitlines()
		f.close()

		f					=	open("./Common/data/PreProcessor/universal_stopwords.txt")
		universal_stopwords	=	f.read().splitlines()
		f.close()

		customstopwords	=	stop	+	universal_stopwords	+	languageStopWords 

		t	=	[w for w in t if not w in customstopwords]

		return ' '.join(t)

	return ''


def pre_processor(tweet, stop_words=None, keep_hashtags=None, keep_mentions=None, stem=None, language = None):

	tweet		=	tweet.upper()

	tweet		=	tweet.replace('"', "")
	tweet		=	tweet.replace("‘","'")
	tweet		=	tweet.replace("’","'")


	tweet		=	strip_underscore(tweet)
	tweet		=	replace_expressions(tweet,language)


	tweet		=	pre_format(tweet, keep_hashtags=keep_hashtags, keep_mentions=keep_mentions )

	if stop_words and language:
		tweet	=	remove_stopWords(tweet,language)

	if stem:
		tweet	=	stem(tweet)

	t			=	tweet.split()


	return	t
    



def pre_format(tweet, keep_hashtags=None, keep_mentions=None):

	if	keep_hashtags:
		tweet	=	tweet.replace("#","")

	if	keep_mentions:
		tweet	=	tweet.replace("@","")

	tweet	=	tweet.replace("5G","XCINQGX")
	tweet	=	tweet.replace("Ç","XCEDILHAX")
	tweet	=	tweet.replace("PAÍS","XPAISX")
	tweet	=	tweet.replace("PAÍSES","XPAISX")
	tweet	=	tweet.replace("PAISES","XPAISX")
	
	tweet	=	re.sub(r'\w+\…', '',tweet)

	tweet	=	 re.sub(' +', ' ',tweet)
	tweet	=	tweet.split(' ')
	tweet	=	[x for x in tweet if x]
	tweet	=	[w for w in tweet if not hasNumbers(w)]	
	tweet	=	[w for w in tweet if len(w)<30]
	tweet	=	' '.join(tweet)
	
	regex	=	r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
	tweet	=	re.sub(regex, ' \g<0>', tweet)

	tweet	=	remove_url(tweet)

	tweet	=	re.sub(r'\d{4}\-\d{2}\-\d{2}\s+\d{1,2}\:\d{2}.*',"",tweet) # remove datetime
	#tweet	=	re.sub(" \d+", "",tweet) #remove numbers

	tweet	=	remove_accents(tweet)
	tweet	=	p.clean(tweet)
	tweet	=	strip_punctuation(tweet)



	tweet	=	tweet.replace("XCEDILHAX","Ç")
	tweet	=	tweet.replace("XPAISX","PAÍS")
	tweet	=	tweet.replace("XCINQGX","5G")

	return tweet







