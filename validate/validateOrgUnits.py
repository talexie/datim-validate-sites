# Validate submitted template files for DHIS2 Metadata
# @author: Alex Tumwesigye
# @email: atumwesigye@gmail.com
# @Date: 10-04-2018
# @Organisation: JSI/ATC

import os
import numpy as np
import pandas as pd,jellyfish as jf
import json
import requests
import moment
import chardet

class ValidateOrgUnits:
	"""docstring for ValidateOrgUnits"""
	def __init__(self):
		super(ValidateOrgUnits, self).__init__()
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
	# Get encoding from files
	def getEncoding(self,fileName):
		openFile = open(fileName,'rb').read()
		result = chardet.detect(openFile)
		characterEncoding = result['encoding']
		return characterEncoding

	# Read in Panda file
	def getPdFile(self,fileName,type):
		df = []
		if type == 'csv':
			fileName="{}.{}".format(fileName,type)
			df = pd.read_csv(fileName,encoding='ISO-8859-1')
		elif type == 'json':
			fileName="{}.{}".format(fileName,type)
			df= pd.read_json(fileName)
		else:
			pass
		return df

    # create Panda Data Frame from event data
	def createDataFrame(self,events,type):
		cols = self.createColumns(events['headers'],type)
		dataFrame = pd.DataFrame.from_records(events['rows'],columns=cols)
		return dataFrame
	#Validate sites
	def validateSites(self,reference=None,validate=None,type='csv',leftColumns=None,rightColumns=None):
		if reference is not None and validate is not None:
			validated=pd.merge(validate,reference,how='left',left_on=leftColumns,right_on=rightColumns)
			return validated
		else:
			print("Specify all required paramters (reference and validate)")
			return None
	# Check duplicates
	# keep { first, last, False }
	def markDuplicate(self,source=None,columns=None,label="Duplicated",keep=False):
		if source is not None and columns is not None:
			source[label] = source.duplicated(subset=columns,keep=keep)
			return source
		else:
			print("Specify source and columns")
			return source

	# Extract MOH ID or MOH Name, etc from site details
	def extractAttributeValue(self,siteAttributeValues=None,type='MOH ID',idScheme='name'):
		attrValue =""
		if(len(siteAttributeValues) > 0) and siteAttributeValues is not None:
			for siteAttributeValue in siteAttributeValues:
				if idScheme.lower() == 'id':
					if(siteAttributeValue['attribute']['id'] == type):
						attrValue = siteAttributeValue['value']
						return attrValue
				elif idScheme.lower() == 'code':
					if(siteAttributeValue['attribute']['id'] == type):
						attrValue = siteAttributeValue['value']
						return attrValue
				else:
					if(siteAttributeValue['attribute']['name'] == type):
						attrValue = siteAttributeValue['value']
						return attrValue

		return attrValue

	# Check if donor was created before receptor
	# return True if receptor is older
	def checkAge(self,row,receptorColumn=None,donorColumn=None):
		siteOld = False
		if(str(row[receptorColumn]) > str(row[donorColumn])):
			siteOld = True
			return 	siteOld
		return siteOld

	# Check if a donor is given as receptor or vice versa
	def checkDuplicatesWithInSites(self,currentRow,df,donor=None,receptor=None):
		site = ""
		count = 0
		for index,row in df.iterrows():
			if(currentRow[donor] == row[receptor]):
				count = count + 1
				if (count > 1):
					site = row[receptor]
					return 	site
		return site

	# Analyse duplicates based on input csv file or same system
	def analyzeDuplicates(self,reference=None,validate=None,type=None,leftColumns=None,rightColumns=None,duplicated=None,dupColumns=None,expression=None):
		merged = None
		if type.lower() ==  "csv":
			if duplicated.lower() == 'keep':
				merged = pd.merge(validate,reference,how='left',left_on=leftColumns,right_on=rightColumns)
				merged['duplicated'] = merged.duplicated(subset=dupColumns,keep=False)
			elif duplicated.lower() == 'remove':
				merged_with_duplicates = pd.merge(validate,reference,how='left',left_on=leftColumns,right_on=rightColumns)
				merged = merged_with_duplicates.drop_duplicates(subset=dupColumns)
			else:
				merged = pd.merge(validate,reference,how='left',left_on=leftColumns,right_on=rightColumns)
		elif type.lower() == 'system':
			merged = reference
			merged['duplicated'] = merged.duplicated(subset=dupColumns,keep=False)
		return merged
	# Get sites by orgUnit
	def getSitesByOrgUnitName(self,orgUnitName,url,username,password):
		url = url+"organisationUnits"
		params = {"fields": "id,code,name,created,parent[id,code,name]","filter":"ancestors.name:ilike:" + orgUnitName,"paging":"false"}
		data = requests.get(url, auth=(username, password),params=params)
		if(data.status_code == 200):
			return data.json()
		else:
			return 'None'
	# Get sites
	def getSites(self,url,username,password):
		url = url + "organisationUnits"
		params = {"fields": "id,code,name,ancestors[id,code,name]","paging":"false"}
		data = requests.get(url, auth=(username, password),params=params)
		if(data.status_code == 200):
			return data.json()
		else:
			return []
	# create Output files
	def createResultFile(self,values,folder,filename,type=None):

		if type=='csv':
			filename = "{}.{}".format(filename,type)
			values.to_csv(os.path.join(self.fileDirectory,folder,filename), sep=',', encoding='utf-8')
		else:
			filename = "{}.{}".format(filename,type)
			values.to_json(os.path.join(self.fileDirectory,folder,filename),orient='records')
		return 'SUCCESS'
	# Rename columns in a dataframe
	def renameColumns(self,source=None,columns=None):
		return source.rename(index=str,columns=columns)
	# Compare strings using Jaro Distance
	def getJaroDistance(self,left=None,right=None):
		return (jf.jaro_distance(left,right)* 100)
	# start validation
	def startValidation(self,folder=None,fileName=None,type='csv'):
		df = self.getPdFile(fileName,type)
		authParam = self.getAuth()
		#Validate without OU
		sites = self.getSitesByOrgUnitName(authParam['orgUnit'],authParam['url'],authParam['username'],authParam['password'])
		dfSites = pd.io.json.json_normalize(sites['organisationUnits'])
		if(len(sites) > 0):
			if len(df['donor_uid'].value_counts() > 0):
				validatedSites = self.validateSites(validate=df,reference=dfSites,type='csv',leftColumns=['donor_uid'],rightColumns=['id'])
				donorSites = self.renameColumns(source=validatedSites,columns={"id":"ref_donor_id","name":"ref_donor_name","code":"ref_donor_code","parent.id":"ref_donor_parentid","parent.code":"ref_donor_parentcode","parent.name":"ref_donor_parentname","created":"ref_donor_created"})
				validSites = self.validateSites(validate=donorSites,reference=dfSites,type='csv',leftColumns=['site_uid'],rightColumns=['id'])
				validSites['Donor Exists'] = validSites['ref_donor_id'].notna()
				validSites['Donor Duplicated'] = validSites.duplicated('donor_uid',keep=False)
				validSites['Possible Donor Duplicates'] = validSites.duplicated(subset=['ref_donor_name','ref_donor_parentid'],keep=False)
				validSites['Site in Donors'] = validSites.apply(self.checkDuplicatesWithInSites,args=(validSites,'donor_uid','site_uid'),axis=1)
				validSites['Donor in Sites'] = validSites.apply(self.checkDuplicatesWithInSites,args=(validSites,'site_uid','donor_uid'),axis=1)
				# Only useful if type of operation is MERGE,DELETE
				validSites['Donor Created Earlier than Site'] = validSites.apply(self.checkAge,args=('created','ref_donor_created'),axis=1)
				validSites['Site Exists'] = validSites['id'].notna()
				validSites['Possible Site Duplicates'] = validSites.duplicated(subset=['name','parent.id'],keep=False)
			else:
				validSites = self.validateSites(validate=df,reference=dfSites,type='csv',leftColumns=['site_uid'],rightColumns=['id'])
				#validSites = self.validateSites(validate=df,reference=dfSites,type='csv',leftColumns=['site_name'],rightColumns=['name'])
				#
				validSites['Site Exists'] = validSites['id'].notna()
				validSites['Possible Site Duplicates'] = validSites.duplicated(subset=['name','parent.id'],keep=False)

			fullValidSites = self.analyzeDuplicates(reference=dfSites,type='system',dupColumns=['name','parent.id'])
		else:
			pass
		outputFile = fileName + "_"+ authParam['orgUnit'] +"_"+ self.today
		outputFileEntireDuplicates = 'Full_' + fileName + "_"+ authParam['orgUnit'] +"_"+ self.today
		self.createResultFile(validSites,folder,outputFile,type)
		self.createResultFile(fullValidSites,folder,outputFileEntireDuplicates,type)
# Start the validation process
if __name__ == "__main__":
	checkSites= ValidateOrgUnits()
	checkSites.startValidation(folder='validations',fileName='test',type='csv')
#main()
