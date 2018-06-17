# Search engine for guardian news
This repository contains code for
* Build Guardian news article database
* Index news article
* Search article based on keywords

Guardian has api service for both commercial and non-commercial purposes for their upto 2 million news article. Non-commercial version is restricted to 5000 calls per day and 12 requests per second. API documentation is available on https://open-platform.theguardian.com/

Given daterange PopulateNewsDB.py creates a table containing following columns
* News Url
* Publication Date
* Headline
* News Body

CreateIndex.py indexes news article and populates necessary tables that will be used for search.

Search.py searches for news article given query. Content based ranking described in "Programming Collective Intelligence" book chapter 4 was used.
