# merge submitted template files
# @author: Alex Tumwesigye
# @email: atumwesigye@gmail.com
# @Date: 10-04-2018
# @Organisation: JSI/ATC, DATIM

import pyexcel as pe
import os
import inspect
import numpy as np
import pandas as pd
import json
import requests
import moment

class MergeFiles:
	"""docstring for MergeFiles"""
	def __init__(self):
		super(MergeFiles, self).__init__()
		self.excelFileName = 'fileName'
		self.currentDir = os.getcwd()
		self.fileType = 'csv'
		self.fileDirectory = os.path.abspath(os.path.dirname(__file__))
		self.today = moment.now().format('YYYY-MM-DD')
		self.path = os.path.abspath(os.path.dirname(__file__))
		newPath = self.path.split('/')
		newPath.pop(-1)
		newPath.pop(-1)
		self.fileDirectory = '/'.join(newPath)
		
		self.fileName = os.path.join(self.fileDirectory,'sites.csv')
		self.orgUnitName ='Global'
	# Get Authentication details
	def getAuth(self):
		with open(os.path.join(self.fileDirectory,'.secrets.json'),'r') as jsonfile:
			auth = json.load(jsonfile)
			return auth
	# Get the sheets as dictionaries from the workbook
	def getBookDict(self,fileName):
		bookDict = pe.get_book_dict(filename=fileName)
		return bookDict

	# Get sheet names
	def getSheetNames(self,book):
		return book.keys()
	# Read in Panda file
	def getPdFile(self,fileName,type):
		df = []
		if type == 'csv':
			df = pd.read_csv(fileName,encoding='utf-8')
		elif type == 'json':
			df= pd.read_json(fileName)
		else:
			pass
		return df

    # create Panda Data Frame from event data
	def createDataFrame(self,events,type):
		cols = self.createColumns(events['headers'],type)
		dataFrame = pd.DataFrame.from_records(events['rows'],columns=cols)
		return dataFrame

	# Read donor and receptor ids
	def getDonorsAndReceptors(self,book):
		content = {"SiteUpdates":[]}
		for sheet,sheetData in book:
			for idx,row in sheetData:
				content['SiteUpdates'].append(row)
		sitesBook = pe.Book(content)
		sitesBook.save_as("Sites Updates.xlsx")
		return "Site Updates file processed"
	# Check Uid exists
	def checkSite(self,uid):
		site = ""
		return site
	# Verify site
	def verifySite(self,row,sites):
		seen = "False"
		if(len(sites['organisationUnits']) > 0):
			for site in sites['organisationUnits']:
				if(row['organisationunituid'] == site['id']):
					seen= "True"
					return seen
		return seen
	# check multiple sites
	def checkMultiSites(self,uids):
		return sites
	# check site name
	def checkSiteName(self):
		return True
	# check parent
	def checkParent(self,uid,parent):
		return True
	# validate receptor if it was created before donor
	# return True if receptor is older
	# 
	def validateReceptor(self,donor,receptor):
		#if(diff between donor created and receptor created):

		return True
	# Get sites by orgUnit
	def getSitesByOrgUnitName(self,orgUnitName,url,username,password,params):
		url = url+"organisationUnits.json?paging=false&fields=id,name,code,ancestors[id,name,code]&filter=name:eq:"+orgUnitName
		
		data = requests.get(url, auth=(username, password),params=params)
		if(data.status_code == 200):
			return data.json()
		else:
			return 'HTTP_ERROR'
	# Get sites
	def getSites(self,url,username,password):
		url = url + "organisationUnits"
		params = {"fields": "id,code,name,ancestors[id,code,name]","paging":"false"}
		data = requests.get(url, auth=(username, password),params=params)
		if(data.status_code == 200):
			return data.json()
		else:
			return []
	# start validation
	def startValidation(self):
		df = self.getPdFile(self.fileName,self.fileType)
		authParam = self.getAuth()
		sites = self.getSites(authParam['url'],authParam['username'],authParam['password'])
		if(len(sites) > 0):
			df['Exists'] = df.apply(self.verifySite,args=([sites]),axis=1)
		else:
			pass
		outputFile = self.fileName + "_"+ self.today+ ".csv"
		df.to_csv(os.path.join(self.fileDirectory,outputFile), sep=',', encoding='utf-8')
# Start the idsr processing
if __name__ == "__main__":
	checkFile= MergeFiles()
	checkFile.startValidation()
#main()

