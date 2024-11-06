import os,code,re
import csv
import multiprocessing as MP
import json

from pprint import pprint

import pandas
import IPython
import datetime
import time


from src import core, opentext2, hwmeta



class HWOTReport(core.Core):
	
	
	def __init__(self,**kargs):		 
		super().__init__(**kargs)  
		self.cen = kargs.get("cen") or core.Central()				 
		self.ot_client = opentext2.OpentextInfo(station="HYPERWAVE", username ="s-hw_mig_opentext@opentext",cen=self.cen)		 
		self.hw_client = hwmeta.HWClient(station="HYPERWAVE", username ="s-hw_mig_opentext@opentext",cen=self.cen)
		
		self.con = self.cen.sql_db
		
		
		
		
	def initOTTble(self,reset=False):
		
		cur = self.con.cursor()
		if reset:
			cur.execute("DROP TABLE IF EXISTS ot_data")		   
		cur.execute("""CREATE TABLE IF NOT EXISTS ot_data (
				title		TEXT,
				id			TEXT,
				docnr		TEXT,
				ver_maj		TEXT,
				ver_min		TEXT,
				path		TEXT
				)""")
		
	
	def loadOTData(self,nodeId,path="/",n=0):						 
		ot_data = self.ot_client.ot_subnodeVersions(nodeId)
		cur = self.con.cursor()
		
		
		for node in ot_data:
			
			nodeId =node["data"]["properties"]["id"]				
			
			if node["data"]["properties"]["type"]==0:
				pth = path+node["data"]["properties"]["name"]+"/"
				
				self.loadOTData( nodeId,pth,n+1)
				
			elif node["data"]["properties"]["type"]==144:				 
				values = [
					node["data"]["properties"]["name"],
					node["data"]["properties"]["id"],
					path
				]
				
				node_data = self.ot_client.ot_nodeInfo(nodeId,fields=["categories","versions"])				   
				values.append( node_data["results"]["data"]["categories"][0]["556452_14"] ) # docnr
				
				for v in node_data["results"]["data"]["versions"]:
					vals = values[:]
					vals+= [v['version_number_major'], v['version_number_minor']] 
					try:
						cur.execute("INSERT INTO ot_data (title, id, path, docnr,ver_maj,ver_min) VALUES (?,?,?,?,?,?)",vals)
					except Exception as err:
						code.interact(local=locals())
						raise err
		self.con.commit()
		
		
		
	def initHWTable(self,reset=False):
		
		cur = self.con.cursor()
		if reset:
			cur.execute("DROP TABLE IF EXISTS hw_data")		   
		
		cur.execute("""CREATE TABLE IF NOT EXISTS hw_data (
				title		TEXT,
				id			TEXT,
				docnr		TEXT,
				ver_maj		TEXT,
				ver_min		TEXT,
				path		TEXT
				)""")
		
		
	def LoadHWData(self,hwID,path="/",n=0):
		
		
		
		self.hw_client.getSessionID()
		res = self.hw_client.getObject(hwID)		
		
		
		self.log.info(("-"*n) + f"Loading {hwID}")
		cur = self.con.cursor()
		
		try:
			#is item is a folder?					 
			if res[0].get('DocumentType') == 'collection':
				
				self.log.info( ("-"*n) + f"-Folder ({hwID})")
				
				#process folder
				path+=res[0].get("HW_ObjectName","noObjNm")+"/"
				
				self.log.info("-"*n+f"-Processing children of {hwID}")
								
				for item in self.hw_client.getChildren(hwID):
					self.LoadHWData( item["HW_OID"],path,n+1)
				self.con.commit()
				
			#is item is a file?			   
			elif res[0].get('DocumentType') == 'Generic':
				self.log.info(("-"*n) + f"-Processing {hwID} as a file")
				self.loadHWData_processFile(hwID,cur,path)
				
			
			
			
			#what is it?
			else:
				code.interact(local=locals())				 
				raise Exception("unknown file type")
		
			
			if n==0:
				self.con.commit()
		except Exception as err:
			raise err
		self.con.commit()
		

	def loadHWData_processFile(self,hwID,cur,path):
		
		try:
			#need to process per version
			_,baseId,exp = hwID.split("-")
			baseId = f"{_}-{baseId}"
			exp = int(exp)
			
			
			
			
			for n in range(exp):
				meta,_ = self.hw_client.getObject(f"{baseId}-{n+1}")
				
				if not meta:
					continue
				try:				
					title = re.match(".+:(.+)",meta["Title"]).group(1)
				except Exception as err:
					print(type(err))				
					ttitle = meta["Title"]
				
				try:
					ver_maj, ver_min = meta["HW_Version"].split(".")
				except (KeyError, TypeError, ValueError) as err:
					ver_maj = -1
					ver_min = -1
				
				
				
				cur.execute("INSERT INTO hw_data (title, id, path, docnr,ver_maj,ver_min) VALUES (?,?,?,?,?,?)",[
					title,
					meta["HW_OID"],
					path,
					meta["DocNumber"],
					ver_maj,
					ver_min
				
				])
		#for general debug purposes
		except Exception as err:
			from pprint import pprint			 
			code.interact(local=locals())
			
			
		
		
		
	
		
	def loadOTData_processFolder(self,*args,**kargs):
		pass
	

class DataFileReport(core.Core):
	
	
	
	def runCheckFilesMigrated(self,data,results=None):		  

		results = results or {"missed":[],"completed":0}
		
		
		
		for f in data["files"]:
			if f.get("ot_node_id",False) == False:
				results["missed"].append( f["HW_OID"] )
			else:
				results["completed"]+=1
				
		for f in data["folders"]:
			self.runCheckFilesMigrated(f,results)

		return results



	def checkMetaData(self,data,n=0):
		rpt = {"done":[],"error":[],"not_done":[],"folders_total":0,"folders_done":[]}
		total_folders = 0
		
		for f in data["files"]:
			
			if	f.get("migrated",False) == True:
				rpt["done"].append(f["HW_OID"])
				
				
			elif f.get("migrated",None) != None:				
					rpt["error"].append(f["migrated"])					  
			else:
				rpt["not_done"].append(f["HW_OID"])
		
		
		for fld in data["folders"]:
			fld_rpt  = self.checkMetaData(fld,n=n+1)			
			
			rpt["done"]+=fld_rpt["done"]
			rpt["error"]+=fld_rpt["error"]
			rpt["folders_done"]+=fld_rpt["folders_done"]
			rpt["folders_total"]+= fld_rpt["folders_total"]+1
			rpt["not_done"]+= fld_rpt["not_done"]
			
			if fld["self"].get("migrated")==True:
				rpt["folders_done"].append( fld["self"]["HW_OID"] )
				
		   
		if n==0:
			pass
			#code.interact(local=locals())
			
		
			
		return rpt
	
	def metaToExcel(self,data,df=None,n=0,path="",parent="root"):
		
		
		if n==0:
			self.df = pandas.DataFrame([],columns=[
				"hwid",
				"DocNumber",
				"Version",
				"name",
				"type",
				"parent",
				"migrated",
				"folders",
				"files",
				"depth",
				"path"
			])
			df = self.df
		
		
		try:
				
			row = [
				data["self"].get('HW_OID'), 
				data["self"].get('DocNumber',""), 
				data["self"].get('HW_Version',""), 
				data["self"].get('HW_ObjectName'), 
				data["self"].get('DocumentType'), 
				parent,
				data["self"].get('migrated',False), 
				len(data["folders"]),
				len(data["files"]),
				n,
				path
			]		 
			try:
				df.loc[len(df.index)] = row
			except Exception as err:
				print(row)
				raise err
						
			for f in data["files"]:
				df.loc[len(df.index)] = [
					f.get('HW_OID'), 
					f.get('DocNumber',"--"), 
					f.get('HW_Version',"--"), 
					f.get('HW_ObjectName'), 
					f.get('DocumentType'), 
					data["self"].get('HW_OID'),
					f.get('migrated'), 
					0,
					0,
					n+1,
					path+"/"+data["self"]["HW_ObjectName"]
				]		 
				
			
			for f in data["folders"]:
				self.metaToExcel(
					data =f,
					df = df,
					n = n+1,
					path = path+"/"+data["self"]["HW_ObjectName"],
					parent = data["self"].get('HW_OID')
					)
				if n < 4:
					self.log.info(f"level: {n}, {' '*n } { f['self']['HW_OID'] }")
					
		
		except KeyboardInterrupt as err:
			self.log.error(f"Emergency Stop ( n={n} )")		   
			if n!=0:
				raise err

		while n==0:
			try:		
				fl = f'output/HW/data_to_excel_{data["self"].get("HW_OID","-" )}.xlsx'
				df.to_excel(fl)
				break
			except Exception as err:
				self.log.error(err)
				ans = input("Could not save. try again?").lower()
				if ans !='y':
					break
		
		return df
	
	def __del__(self):
		try:
			self.csvdoc.close()
		except AttributeError:
			pass




class CSVReports(core.Core):
		
	def __init__(self,**kargs):
		super().__init__(**kargs)
		raise Exception("Under Construction")
		
		
	def setDataFile(self,fl):
		self.data_file = fl
	
	def  run(self,data,csv=None,n=0):
		
		fl = os.path.splitext( os.path.split(self.data_file)[1] )[0]
		

class OTTreeReport(core.Core):
	
	
	def __init__(self,**kargs):
		super().__init__(**kargs)
		self.nodeId = kargs["nodeId"]
		
		
		self.username = kargs.get("username","s-hw_mig_opentext@opentext")
		self.server = kargs.get("server","http://opentextcs.eskom.co.za/otcs/cs.exe/")
		
		self.ot = kargs.get("ot",False) or opentext2.OpentextInfo( username = self.username, url_base = self.server)
		
		
		self.initData(kargs.get("datafile"),kargs.get("data"))
		
		
		self.datasaver = DataSaver()  
		self.datasaver.runState.set()
		self.datasaver.start()
		
	
	def initData(self,datafile,data):		 
		self.data = {"self":{},"folders":{}}
		
		if data:
			self.data = data
		elif datafile:
			try:
				self.data = self.J(datafile)
			except (FileNotFoundError, json.decoder.JSONDecodeError):
				self.log.error("Datafile not found resetting")
			
	
	def stopDataSaver(self):
		try:
			os.remove( f"pid/{self.datasaver.pid}" )
		except FileNotFoundError:
			pass
		
	def run(self):
		try: 
			while self.datasaver.ready.is_set() == False:
				pass
			self.ot.ot_ticket()
			data =	self.makeTreeR()		
			self.JD(data, "output", "ot",f"{self.nodeId}_tree.json" )			 
			self.stopDataSaver()
		except Exception as err:
			self.JD(self.data, "output", "ot",f"{self.nodeId}_tree_errored.json" )
			self.stopDataSaver()
			raise err
		
		
	
	def backupReportFile(self,fl):
		dt = datetime.datetime.now().strftime("%Y%m%d%H%M")
		
		
		dir_back = os.path.join("output", "ot","bak",f"{self.nodeId}")
		fl_back  = os.path.join( dir_back, f"{self.nodeId}_tree_{dt}.json")
		
		#make sure back folder exists		  
		os.makedirs( dir_back, exist_ok=True)		 
		
		#make backup 
		try:
			self.BW( self.B( fl ), fl_back )		
			for f in sorted(os.listdir(dir_back),reverse=True)[3:]:
				os.remove(os.path.join(dir_back,f))		   
		except FileNotFoundError:
			pass
	
	def updatDataFile(self):
		
		if self.datasaver.ready.is_set():  
			self.log.info("Saving data")	  
			fl = os.path.join( "output", "ot",f"{self.nodeId}_tree.json")
			# ~ self.backupReportFile(fl)
			self.datasaver.qin.put((fl,self.nodeId,self.data))
			
		# ~ self.JD(self.data, fl )
	
	
	def makeTreeR(self,nodeId=None,n=0,data=None,path="/"):
				
		try:			
			data = data or self.data
			if data["self"].get("rpt_done",False) == True:
				return data
			
			
			self.log.debug(f'processing {nodeId}  {data.get("sefl",{}).get("id","")  }' )
			
			
			
			if nodeId == None:
				nodeId = self.nodeId
			
			if data["self"] == {}:				
				node_data = self.ot.ot_nodeInfo(nodeId)			   
				d = node_data["results"]["data"]["properties"]			
				self.log.debug(f"{'  '*n}{path}")			
				self.log.debug(f"{'  '*n}{d['name']} no data")			
				
				
				
				
				for k in ["id","name","parent_id","type","type_name"]:
					data["self"][k] = d[k]
			
			
			for node in self.ot.ot_subnodeVersions(nodeId):					   
				
				if node["data"]["properties"]["type"] == 0:				   
					#child is a folder:
					
					child_id = node["data"]["properties"]["id"]					   
					child_data = data["folders"].get( f"{child_id}" ,{"self":{},"folders":{}})
					
					if child_data["self"] == {}:
						self.log.debug(f"{'  '*n}No data for child {child_id}")

				
					data["folders"][child_id] = child_data
					
					self.makeTreeR( child_id, n+1, data=child_data, path=path+f"/{child_id}" )
					
					
				else:
					
					#child is not a folder assume some kind of file:					
					
					child_data = {"self": node["data"]["properties"] }
					child_data["versions"] = node["data"].get("versions",[])
					
					child_type = child_data["self"]["type_name"]
					if child_type == "Folder":
						pprint(child_data)
						raise Exception("Child is FOLDER")
					
					
						
										
					try:
						data[ child_type ].append(child_data)
					except KeyError:
						data[ child_type ] = [ child_data ]
					
			self.log.info(f'{"	"*n} {data["self"]["name"]} ({data["self"]["id"]}) done')
			data["self"]["rpt_done"] = True
			self.updatDataFile()
		except Exception as err:			
			if n!=0:
				raise err
			else:
				if type(err) != KeyboardInterrupt:
					self.log.critical("Could not generate report")
					self.data = data
					raise err
					
		if n==0:
			self.log.info("N=0 Done")
			
		return data
			


class OTCSVReport(core.Core):
	
	def __init__(self,**kargs):
		super().__init__(**kargs)		 
		self.data = kargs["data"]
		self.nodeId = kargs["nodeId"]
		
		self.w = kargs.get("w",None)		
		self.reportFile = os.path.join( "output","ot",f"csv_{self.nodeId}.csv" )
		
		self.header = ["id","name","type",'file_name',"ver_maj","ver_min","parentId","path"]
		
		
		
	def run(self):
		self.initCSVFile()
		self.makeReportR()
		
	
	
	def backupReportFile(self):
		dt = datetime.datetime.now().strftime("%Y%m%d%H%M")
		dir_back = os.path.join(
					os.path.split( self.reportFile )[0],
					"bak",
					f"{self.nodeId}"
					)		 
		fl_back = os.path.join( dir_back, "csv_{self.nodeId}_{dt}.csv")
		try:
			self.BW( self.B( self.reportFile),fl_back)
		except FileNotFoundError:
			self.log.warning("no report file found to backup")
			return False
		
		for f in sorted(os.listdir(dir_back),reverse=True)[3:]:
			os.remove( os.path.join( dir_back,f))
		return True
		
	
	def initCSVFile(self):
		file_exists = self.backupReportFile()
		
		if self.w == None:
			self.csvfile = open( self.reportFile,"at",newline="\n",encoding="utf-8")
			self.w = csv.writer( self.csvfile )
				
		if file_exists == False:
			self.w.writerow(self.header)
		
	
	def makeReportR(self,data=None,n=0,path="",parentId =""):
		
		self.nodeId
		if self.w==None:
			self.initCSVFile()
			
		data = data or self.data	
		
		self.w.writerow([
			data["self"]["id"],
			data["self"]["name"],
			data["self"]["type_name"],
			"",  "","",
			parentId,
			path		
		])
		
		for fld in data["folders"]:
			self.makeReportR(
				data = data["folders"][fld],
				n = n+1,				
				path = f'{path}{data["self"]["name"]}/',
				parentId = data["self"]["id"]
			)
			
		for k in data:
			if k=="self" or k=="folders":
				continue
			for doc in data[k]:
				
				row0 = [
					doc["self"]["id"],
					doc["self"]["name"],
					doc["self"]["type_name"],
					]			 
				for ver in doc["versions"]:
					self.w.writerow( row0+[
							ver['file_name'],
							ver['version_number_major'],
							ver['version_number_minor'],  
							data["self"]["id"],
							path+data["self"]["name"]						   
							])
		if n==0:
			self.log.info("N=0 Done")
			self.csvfile.close()
	
		
		
class DataSaver(MP.Process):
	
	
	def __init__(self):
		super().__init__()
		self.qin = MP.Queue()
		self.ready = MP.Event()
		self.interval = 1*60
		
		self.runState = MP.Event()
		self.DATA_BLOCK  = 1024 *1024 *100
	
	def backupReportFile(self,fl,nodeId):
		
		dir_report_file,report_file_name = os.path.split(fl)
		report_file_name,ext  = os.path.splitext(report_file_name)
		
		dir_back = os.path.join(dir_report_file,"bak",f"{nodeId}")
		dt = datetime.datetime.now().strftime("%Y%m%d%H%M")
		report_file_name_backup = f'{report_file_name}_{dt}{ext}'
		
		os.makedirs( dir_back, exist_ok= True)		  
		
		try:
			with open(fl,"rb") as inf:
				with open(os.path.join(dir_back,report_file_name_backup),"wb") as outf:				   
					data = inf.read(self.DATA_BLOCK)
					while data:
						outf.write( data )
						data = inf.read(self.DATA_BLOCK)
			
			
			for f in sorted(os.scandir(dir_back),reverse=True,key=lambda x:x.name)[3:]:
				os.remove(f.path)
			return True 
				   
		except FileNotFoundError:		 
			return False
			
	def run(self):
		with open(f"pid/{self.pid}","wt") as  pid:
			pass
		
		while os.path.exists( f"pid/{self.pid}" ):
			self.ready.set()
			
			args = self.qin.get()
			if args == None:
				break
			
			fl,nodeId,data = args
			self.backupReportFile(fl,nodeId)
		
			
			with open(fl,"wt") as jout:					   
				json.dump(data,jout)		
			self.ready.clear()			
			time.sleep(self.interval)
		if os.path.exists( f"pid/{self.pid}" ):
			os.remove(	f"pid/{self.pid}" )
		print("DATA SAVER DONE")

		


