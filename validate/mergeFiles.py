# merge submitted template files
# @author: Alex Tumwesigye
# @email: atumwesigye@gmail.com
# @Date: 10-04-2018
# @Organisation: JSI/ATC

#import pyexcel as pe
import os
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
		
		self.fileName = os.path.join(self.fileDirectory,'orgunits.csv')
		self.orgUnitName ='Global'
	# Get Authentication details
	def getAuth(self):
		with open(os.path.join(self.fileDirectory,'.secrets.json'),'r') as jsonfile:
			auth = json.load(jsonfile)
			return auth

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

	# Get donor details
	def getSiteDonorParent(self,row,sites,type):
		siteParent = ""
		for key,site in sites.iterrows():
			if(row['donor_or_current_site_DATIM_uid'] == site['id']):
				if type == 'PARENTID':
					siteParent =site['parent.id']
					return 	siteParent
				elif type == 'PARENTNAME':
					siteParent = site['parent.name']
					return 	siteParent
				elif type == 'MOHID':
					siteParent = self.extractMOHID(site['attributeValues'])
					return 	siteParent
				elif type == 'CREATED':
					siteParent = site['created']
				elif type == 'VERIFYSITE':
					siteParent = 'True'
					return 	siteParent
				else:
					pass
			
		return siteParent
	# Extract MOH ID from site details
	def extractMOHID(self,siteAttributeValues):
		attrValue =""
		if(len(siteAttributeValues) > 0):
			for siteAttributeValue in siteAttributeValues:
				if(siteAttributeValue['attribute']['name'] == 'MOH ID'):
					attrValue = siteAttributeValue['value']
					return attrValue
		return attrValue
	# Get receptor details
	def getSiteReceptorParent(self,row,sites,type):
		siteParent = ""
		for key,site in sites.iterrows():
			if(row['receptor_site_DATIM_uid'] == site['id']):
				if type == 'PARENTID':
					siteParent = site['parent.id']
					return 	siteParent
				elif type == 'PARENTNAME':
					siteParent = site['parent.name']
					return 	siteParent
				elif type == 'MOHID':
					siteParent = self.extractMOHID(site['attributeValues'])
					return 	siteParent
				elif type == 'CREATED':
					siteParent = site['created']
				elif type == 'VERIFYSITE':
					siteParent = True
					return 	siteParent
				else:
					pass
			else:
				pass

		return siteParent
	# Check if donor was created before receptor
	# return True if receptor is older
	def checkAgeDonor(self,row):
		siteOld = False
		if(row['receptorCreated'] > row['donorCreated']):
			siteOld = True
			return 	siteOld				
		return siteOld
	# Check duplicates
	def checkDuplicates(self,currentRow,df,column):
		site = ""
		count = 0
		for index,row in df.iterrows():
			if(row[column] == currentRow[column]):
				count = count+1
				if(count > 1):
					site = row[column]
					return 	site				
		return site
	# Check if a donor is given as receptor or vice versa
	def checkDuplicatesWithInSites(self,currentRow,df,donor,receptor):
		site = ""
		count = 0
		for index,row in df.iterrows():
			if(currentRow[donor] == row[receptor]):
				count = count + 1
				if (count > 1):
					site = row[receptor]
					return 	site				
		return site

	# 
	# Get sites by orgUnit
	def getSitesByOrgUnitName(self,orgUnitName,url,username,password):
		url = url+"organisationUnits"
		params = {"fields": "id,code,name,created,parent[id,code,name],attributeValues[value,attribute[name]]","filter":"ancestors.name:ilike:" + orgUnitName,"paging":"false"}
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
		#Validate without OU
		sites = self.getSitesByOrgUnitName(authParam['orgUnit'],authParam['url'],authParam['username'],authParam['password'])
		dfSites = pd.io.json.json_normalize(sites['organisationUnits'])
		if(len(sites) > 0):
			df['Donor Exists'] = df.apply(self.getSiteDonorParent,args=(dfSites,'VERIFYSITE'),axis=1)
			df['Receptor Exists'] = df.apply(self.getSiteReceptorParent,args=(dfSites,'VERIFYSITE'),axis=1)
			df['Donor Parent UID'] = df.apply(self.getSiteDonorParent,args=(dfSites,'PARENTID'),axis=1)
			df['Donor MOH ID'] = df.apply(self.getSiteDonorParent,args=(dfSites,'MOHID'),axis=1)
			df['Donor Parent Name'] = df.apply(self.getSiteDonorParent,args=(dfSites,'PARENTNAME'),axis=1)
			df['Receptor Parent UID'] = df.apply(self.getSiteReceptorParent,args=(dfSites,'PARENTID'),axis=1)
			df['Receptor MOH ID'] = df.apply(self.getSiteReceptorParent,args=(dfSites,'MOHID'),axis=1)
			df['Receptor Parent Name'] = df.apply(self.getSiteReceptorParent,args=(dfSites,'PARENTNAME'),axis=1)
			df['Donor Duplicated'] = df.duplicated('donor_or_current_site_DATIM_uid')
			df['Donor Duplicates'] = df.apply(self.checkDuplicates,args=(df,'donor_or_current_site_DATIM_uid'),axis=1)
			df['Receptor in Donors'] = df.apply(self.checkDuplicatesWithInSites,args=(df,'donor_or_current_site_DATIM_uid','receptor_site_DATIM_uid'),axis=1)
			df['Donor in Receptors'] = df.apply(self.checkDuplicatesWithInSites,args=(df,'receptor_site_DATIM_uid','donor_or_current_site_DATIM_uid'),axis=1)
			## Only useful if type of operation is MERGE,DELETE
			df['donorCreated'] = df.apply(self.getSiteDonorParent,args=(dfSites,'CREATED'),axis=1)
			df['receptorCreated'] = df.apply(self.getSiteReceptorParent,args=(dfSites,'CREATED'),axis=1)
			df['Donor Created Earlier than Receptor'] = df.apply(self.checkAgeDonor,axis=1)
			
		else:
			pass
		outputFile = self.fileName + "_"+ authParam['orgUnit'] +"_"+ self.today+ ".csv"
		df.to_csv(os.path.join(self.fileDirectory,outputFile), sep=',', encoding='utf-8')
# Start the idsr processing
if __name__ == "__main__":
	checkFile= MergeFiles()
	checkFile.startValidation()
#main()

