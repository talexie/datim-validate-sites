# Site Validation Scripts

- Validate Sites in multiple sheets
- Extract donor UIDs, receptor UIDs automactically from sheets
- Create a csv file with donor uid,donor parent uid,receptor uid, receptor parent uid (uid base file)
- Validate using created date to check if the donor is older that receptor, check non existing uids, etc
- Generate summarized data for each period
- Generate all data for each period by data element or orgunit
- Generate the structure file and validate against the existing file to remove duplicates
- Proposed python utils [Pandas, openpyxl but pyexcel is preferred as 
- it can generate pandas like dataframes in short codes

# Configure the script
- Copy the csv file to the directory where the cloned validate scripts and name it "sites.csv"
  `--sites.csv
  --.secrets.json
  ----datim-validate-sites (cloned folder)
  `

- Create .secrets.json file with the following parameters
	`{
	  "username": "admin",
	  "password": "district",
	  "url": "https://dhis2.org/instance/api/29/"
	  "orgUnit":"orgUnit Name to act as root"
	}`
- Run the script `python3 validate/mergeFiles.py`
