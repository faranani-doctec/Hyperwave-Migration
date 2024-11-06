from pprint import pprint
import sys
import os
import datetime,code,re
import hashlib 

import datetime

from json import JSONDecodeError
import keyring

import IPython, pandas, requests

from src.core import Core, Central 
from src import opentext2
from src import hwmeta



class Manager(Core):
	def HWRun(self):        
		if True:
			# ~ mm = hwmeta.MongoMigate(cen=self.cen)        
			# ~ mm.run(reset="D")
			
			# ~ root_ot_id = 12073366  #TX Level 2
			# ~ root_ot_id = 13169962  #Nuclear "ID-0.56914813-1"
			# ~ root_ot_id = 13192395  #Request 2024-02-01  ID-0.66708920-1
			# ~ root_ot_id = 13224143  #Request 2024-02-02  ID-0.49652524-1
			# ~ root_ot_id = 13224143  #Request 2024-02-02  ID-0.49652524-1
			
			hwids = [			
				("ID-0.71710556-1",1998085),
				
			]
			
			for hwID,root_ot_id in hwids:            
				fl = hwID.replace(".","_")
				hwr = hwmeta.HWReports(cen = self.cen)        
				data = hwr.rptFileVersionsData(hwID)
				self.JD(data,"output","HW","reportdata",f'{fl}.json')                    
				try:
					data = self.J("output","HW",f'{hwID}_data.json') 
					
				except ( FileNotFoundError, JSONDecodeError ):
					data = self.J("output","HW","reportdata",f'{fl}.json')
				
				hwm = hwmeta.HWMigrator(cen = self.cen)                                
				try:
					hwm.processFromData(data,root_ot_id)
				except KeyboardInterrupt:
					self.log.info("User Stop")
					self.JD(data,"output","HW","reportdata",f'{fl}.json')
				
	def gogo(self):
		hwr = hwmeta.HWReports(cen = self.cen)        
		#IPython.embed()
		breakpoint()
	
	def check(self,):        
		# ~ hwID = "ID-4000.5889129008158-1".replace(".","_")
		
		hwid = "ID-0.56914813-1"
		
		self.log.info("running check")
		
		hwr = reports.DataFileReport(cen = self.cen)                        
		run = True
		
		while run:        
			while True:
				try:                 
					data_raw = self.J("output","HW",f"{hwid}_data.json")				
					break
				except JSONDecodeError:
					self.log.error("decode_error")
			
			data = hwr.checkMetaData(data_raw)
			for k in data:
				try:
					print(k, len( data[k]))    
				except TypeError:
					print(k, data[k])    
			
			
			 
	def info(self):
		
		# nodeId=2442805  #nuclear 
		# ~ nodeId=7837406  #2024-03-25 Rakesh
		# ~ nodeId=13733039  #2024-03-25 Rakesh
		# ~ nodeId = 12350035 #2024-04-02 Rakesh
		# ~ nodeId = 6537066 #2024-04-16 Rakesh
		# ~ nodeId = 12350035 #2024-05-02 Rakesh
		nodeId = 12350035 #2024-06-04 Rakesh
		
		
		otr = reports.OTTreeReport( 
			cen = self.cen,
			nodeId=nodeId,
			datafile=os.path.join("output", "ot",f"{nodeId}_tree.json" )            ,
			#server = "http://svrvmsa1362.elec.eskom.co.za/OTCS/cs.exe/",
			#username = "elec\hw-opentext"
		 )           
		otr.run()         
		
		data = self.J("output","ot",f"{nodeId}_tree.json")        
		ocr = reports.OTCSVReport(cen = self.cen,data=data,nodeId=nodeId)
		ocr.run()

if __name__ == "__main__":        
	
	try:
		arg = sys.argv[1].upper()
	except:    
		#        0     1      2    
		arg = ["HW","INFO","CHECK"][0]

	print(arg)
	
	m  = Manager()
	if arg =="INFO": 
		m.info()
	elif arg =="HW": 
		m.HWRun()
	elif arg =="GOGO": 
		m.gogo()
	elif arg =="CHECK": 
		m.check()


