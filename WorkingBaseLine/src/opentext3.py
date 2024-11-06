import urllib.parse
import getpass
import os
import itertools


import code
import datetime
import json 
import re

import requests
import keyring
import IPython

from pprint import pprint

from src import core





#-----------------------------------------------------------------------
#Exceptions
#-----------------------------------------------------------------------
  
class OTGeneralError(Exception):
	pass

class OTCreateFolderError(OTGeneralError):
	pass

class OTNodeProcessError(OTGeneralError):
	pass

	
class OTSessionExpiredError(OTGeneralError):
	pass
  
class OTExistingNodeNotFoundError(OTGeneralError):
	pass  
class OTRequestError(OTGeneralError):
	pass

#-----------------------------------------------------------------------
#wrapper
#-----------------------------------------------------------------------
def all_pages(f):		
	def wrapper(*args,**kargs):	   
		print(args)
		#ot function
		res  =  f(*args,**kargs)		
		
		#amalgamation of resutls
		data = res["results"]
		try:
			href = res["collection"]["paging"]["links"]["data"].get("next")
		except KeyError as err:
			href = False
	
		while href:
			res = requests.get(			
				url = URL_BASE+href["href"],
				headers = { "otcsticket":args[0].ticket["code"]},		    
				).json()
			
			print("page {} of {}".format(res["collection"]["paging"]["page"], res["collection"]["paging"]["page_total"] ))
			
			href =res["collection"]["paging"]["links"]["data"].get("next",False)
			data +=res["results"]
		return data
	return wrapper


def json_default(*args,**kargs):	
	tp = type( args[0])
	if tp == datetime.datetime:		
		return str( args[0].isoformat() )
		#return str( datetime.datetime.utcfromtimestamp( args[0].timestamp() ) )
	elif tp == bytes:
		return "~bytes~"
		
	else:
		return str(args[0])

#-----------------------------------------------------------------------
#Client
#-----------------------------------------------------------------------


class OpenTextClient(core.Core):
	
	
	def __init__(self,**kargs):
		super().__init__(**kargs)		
		self.url_base   = kargs.get("url_base","http://opentextcs.eskom.co.za/otcs/cs.exe/")		
		self.root_id	= kargs.get("rootId")
		self.defaul_cat = kargs.get("category")
		self.username   = kargs.get("username","s-hw_mig_opentext@opentext")
		self.pwd = kargs.get("pwd",None)

		self.ticket = {"expire":  ( datetime.datetime.now() - datetime.timedelta( seconds=5 ) )}
		
#-----------------------------------------------------------------------
#OT Functions 
#-----------------------------------------------------------------------

	def updateTicket(self):		
		
		if self.ticket == None or self.ticket["expire"] <  datetime.datetime.now():			
			try:
				pswrd = keyring.get_password(self.username,"password" )
			except Exception as err:
				print(err)
				
				pswrd = self.pwd or getpass.getpass(f"Password({self.username}): ")
			
			data = requests.post(
				self.url_base + "api/v1/auth",
				data = [("username", self.username.split("@")[0]), ("password",pswrd) ]
			)		
				
			data = self.resolveResponse( data )
				
			self.log.info("ticket_updated")
			exp = datetime.datetime.now()+datetime.timedelta(minutes=10)
			self.ticket = {"code":data["ticket"],"expire":exp}
		

	def findSubNode(self,parentId,name,versions=False):
		
		url_name = urllib.parse.quote(name)
		url = self.url_base + "api/v2/nodes/{}/nodes?where_name={}".format( parentId, url_name )
		
		self.updateTicket()
		
		ot_data = self.resolveResponse(
			requests.get( url, headers = { "otcsticket":self.ticket["code"] } )
		) 
			  
		for node in ot_data["results"]:
			node = node["data"]["properties"]
			if node["name"] == name:
				return node["id"]
		
		raise OTGeneralError("Node not found")

	
	
	def createFolder(self, parentId,name):
		self.updateTicket()
		#request wrapped in resolveResponse
		ot_data = self.resolveResponse(
			#request
			requests.post(
				url = self.url_base + "api/v2/nodes/",
				headers = { "otcsticket":self.ticket["code"]},		    
				data = {"body":json.dumps({
							"type":0,
							"name":name,
							"parent_id":parentId,
							"versions_control_advanced":True,
							"advanced_versioning":True
					})}	        
				),
			error_ok = True #in case folder exists
		)
		
		
		try:
			node_id = ot_data["results"]["data"]["properties"]["id"]
		except KeyError:				        
			
			if re.match("An item with the name '.+' already exists",ot_data["error"]):
				#folder already exists,get its id 
				node_id = self.findSubNode(parentId,name)
				
			else:			    
				#something else whent wrong
				raise OTCreateFolderError(ot_data["error"])
		try:
			res = requests.put(url = self.url_base + "api/v2/nodes/{}/".format(node_id),
				headers = { "otcsticket": self.ticket["code"]},
				data = {"body":json.dumps({ "advanced_versioning":True,})}		    
			)
		except Exception as err:
			
			raise OTCreateFolderError(f"Could not add versioning to folder.\n reason:{err.args[0]}.\nFolder id:{node_id}")
			
		
		return node_id 
	
		
		
	#---------------------------------------------------------------
	# Create File functions
	#---------------------------------------------------------------
	def createFile(self,parentId,node):		
		
		self.updateTicket()

		ot_data = self.resolveResponse(
			requests.post(
			url = self.url_base + "api/v2/nodes/",
			headers = { "otcsticket":self.ticket["code"]},		    
			data = {"body":json.dumps({
					"type":144,
					"name": node["filename"],
					"parent_id":parentId, 
					"advanced_versioning":True,                   
					"add_major_version": node.get("major",True),
					
					"roles":{"categories":node["roles"]}                    
					},default = json_default)},             
					
			files= {"file":(node["filename"], self.filedataToBin(node["filedata"])  )},
				)
			)
		return ot_data["results"]["data"]["properties"]["id"]
		
				
	
	def deleteVersion(self,nodeid,version):
		res = requests.delete(
			url = self.url_base+"api/v2/nodes/{}/versions/{}".format(nodeid, version),
			headers = { "otcsticket":self.ticket["code"]}
		).json()
		
		try:  
			raise OTNodeProcessError(res["error"])
		except KeyError:
			pass
		
     



				
		
	def mkDummy(self,**kargs):
		return {
			"filename":"dummy.txt",
			"filedata":b'version placeholder',                
			"roles":{},
			"major": kargs["maj_ver"],                
			"dummy":True
		}
	 
	def filedataToBin(self,filedata):
		try:
			filedata.seek(0)
			filedata = filedata.read()
		except Exception as err:       
			if type( filedata ) == str and os.path.exists(filedata):
				filedata = self.B(filedata)
			elif type( filedata ) == str:                
				filedata = bytes.fromhex(filedata)
			elif type( filedata )== bytes:
				filedata = filedata
			else:
				print( filedata )
				raise ValueError("Filedata not in correct format")
		return filedata


	#---------------------------------------------------------------
	#END
	#---------------------------------------------------------------	

	def findSubNodeVersions(self,parentId,name):
		name = name.strip()
		self.updateTicket()
		
		file_name_searched_counter = 0 
		
		#search is made with sorter and sorter versions of the name up to 1/(2*3) the length			
		
		while file_name_searched_counter < 4:
			num_char_to_use = int( len( name ) / ( 2 * file_name_searched_counter or 1 ) )
			search_name = name[ : num_char_to_use ]
			
			url = self.url_base + "api/v2/nodes/{}/nodes?".format(parentId)
			url += "where_name={}".format(name)		
			url += "&fields=versions&fields=properties{id,name}"
			
			search_data = self.resolveResponse(
				requests.get(
					url,
					headers = { "otcsticket":self.ticket["code"]},	              
				)
			)
			
			for search_node in search_data["results"]:			    
				if search_node["data"]["properties"]["name"] == name:
					return search_node
			
			file_name_searched_counter += 1
			
	def updateVersion(self, node,nodeId, version_nr):		
		self.log.info(f"Update version {version_nr} of {nodeId}")

				
		url = self.url_base + f"api/v2/nodes/{nodeId}/versions/{version_nr}"

		data ={ 
			"name": node["filename"],						
		}

		ot_data = self.resolveResponse(
				requests.put(
					url,
					headers = { "otcsticket":self.ticket["code"]},	              
					data = {"body":json.dumps( data )},            
 
					files = {"file":(node["filename"], self.filedataToBin(node["filedata"]) )}
				)
		)
		return ot_data
		
	def addVersion(self,nodeId,node):
		
		url = self.url_base + f"api/v2/nodes/{nodeId}/versions/"
		
		ot_data = self.resolveResponse( 
			requests.post(
				url = url,
				headers = { "otcsticket":self.ticket["code"]},		    
				data = {"body":json.dumps({		           
						"name": node["filename"],                                       
						"add_major_version": node["major"]
						})},
						
				files = {"file":(node["filename"], self.filedataToBin(node["filedata"]) )},
			)
		)
	
	

	def fixFirstVersion(self,otData,node):
		
		ver = otData['results']['data']['versions']
		
		if ver['version_number']==1 and  ver['version_number_major']==0 and node["major"]:
			
			#premote version has a bug. Work around
			# ~ res  = requests.post(
				# ~ url = URL_BASE+f"api/v1/nodes/{nodeId}/versions/1/promote",
				# ~ headers = { "otcsticket":self.ticket["code"]},
				# ~ )		        
			
			self.ot_addVersion(node,nodeId,versionNr=None)			
			self.ot_deleteVersion(nodeId,version=1)
	def subNodes(self,nodeId):
		
		url = self.url_base + f"api/v2/nodes/{nodeId}/nodes"
		ot_data = self.resolveResponse( data = self.allPages( 
			requests.get(
				url = url,
				headers = { "otcsticket":self.ticket["code"]}
		)))
		data = []
		for node in ot_data["results"]:
			data.append( {"properties": node["data"]["properties"], "categories": node["data"]["categories"]}) 
		return data

	def setCategoryInheritance(self,nodeId,catId,enable=True):
		url = self.url_base +f"api/v2/nodes/{nodeId}/categories/{catId}/inheritance"
		fnk = enable and requests.post or requests.delete
		res = fnk(
			url = url,
			headers = { "otcsticket":self.ticket["code"]},
		)
		if not res.ok:
			raise OTRequestError(f"Failed to set inheratance on {nodeId} for cat: {catId}\n {res.reason}")

	def getCategoryValues(self,nodeId):
		""" Args:
				nodeId :Int The node to query
		"""

		ret = {}
		
		url = self.url_base +f"api/v2/nodes/{nodeId}/categories/"
		ot_data = self.resolveResponse( 
			requests.get(
				url = url,
				headers = { "otcsticket":self.ticket["code"]},
			)
		)["reulsts"]["data"]["categories"]
 			
		return ot_data

	def setCategoryValues(self,nodeId,values):
		""" Args:
				nodeId  :Int  	The node to apply the changes to 
				values	:Dict 	The new values to set
		"""
		URL = self.url_base +"api/v1/nodes/{nodeId}/categories/{catId}"


		for catId, vals  in itertools.groupby( sorted(values.keys()), key=lambda x: x.split("_")[0]):
			url = URL.format(nodeId=nodeId, catId=catId)
		
			ot_data = self.resolveResponse( 
				requests.put(
					url = url,
					headers = { "otcsticket" : self.ticket["code"] },
					data = { "body":json.dumps({v:values[v] for v in vals })}
				)
			)


	def getCatergoriesOnNode(self,nodeId):
		url = self.url_base +f"api/v1/nodes/{nodeId}/categories/"
		
		ot_data = self.resolveResponse( 
			requests.get(
				url = url,
				headers = { "otcsticket":self.ticket["code"]},
			)
		)
		cats = []
		for cat in ot_data["data"]:
			cats.append(cat["id"])
		return cats

	def getCategoryInfo(self,nodeId):
		
		catsIds = self.getCatergoriesOnNode(nodeId)
		
		catInfo  ={}
		for catId in catsIds: 
			url = self.url_base +f"api/v1/nodes/{nodeId}/categories/{catId}"
		
			data = self.resolveResponse( 
				requests.get(
					url = url,
					headers = { "otcsticket":self.ticket["code"]},
				)
			)
			catInfo[catId] = {}

			for df in data["definitions"]:
				catInfo[catId][df]={
						"name": data["definitions"][df]["name"],
						"valid":data["definitions"][df]["valid_values"],
						"required":data["definitions"][df]["required"],
						"max_length":data["definitions"][df].get("max_length")
				}


		return catInfo

	def removeCat(self,**kargs):
		
		url = self.url_base + f"api//v2/nodes/{kargs['nodeId']}/categories/{kargs['catId']}"
		ot_data = self.resolveResponse( 
			requests.delete(
				url = url,
				headers = { "otcsticket":self.ticket["code"]},
		))

	def nodeInfo(self,nodeId,allFields=False):
		
		url = self.url_base + f"api/v2/nodes/{nodeId}"
		
		if not allFields:
			url+="?fields=versions&fields=properties"
			
		ot_data = self.resolveResponse( 
			requests.get(
				url = url,
				headers = { "otcsticket":self.ticket["code"]},
		))
		return ot_data


		

	def addCategory(self,**kargs):
		"""
		Args:
			nodeId 		:int 	The id of node to add the cat to 
			data 		:dict  	The metadata to add to the node (optional)
			catId: 		:int 	The id of the cat to apply to the node
			inherent	:bool	add set inheratance for children
			children	:bool	set true to add cats to children 
		"""
		nodeId = kargs["nodeId"]
		data = kargs.get("data",{"category_id": kargs["catId"]})
		catId = kargs.get("catId",data["category_id"])

		url = self.url_base + f"api/v1/nodes/{nodeId}/categories"
		self.updateTicket()

		ot_data = self.resolveResponse( 
			requests.post(
				url = url,
				headers = { "otcsticket":self.ticket["code"]},
				data={"body": json.dumps(data)  }
		),error_ok=True)
		if kargs.get("inherent",False) == True:
			self.setCategoryInheritance(nodeId,catId,enable=True)
			

		if kargs.get("children",False)==True:
			try:
				
				for node in self.subNodes(nodeId):
					su_nodeId = node["properties"]["id"]
					self.addCategory( 
							nodeId=su_nodeId, 
							catId = catId,
							children = kargs.get("children",False),
							inherent = kargs.get("inherent",False),
							n=kargs.get("n",0)+1
						
					)
			except OTRequestError:
				self.log.warning(f"No child nodes for {nodeId} to apply category ({catId})")

	def promoteLastMinorVersion(self,**kargs):

		nodeId = kargs['nodeId']
		ot_data = self.nodeInfo(nodeId)
		
		try:
			ver = sorted(ot_data["results"]["data"].get("versions",[]), key=lambda x:  x["version_number"])[-1]
		
		except IndexError:
			nodeTypeName = ot_data['results']['data']["properties"]["type_name"]
			nodeType = ot_data["results"]["data"]["properties"]["type"]
			
			if kargs.get("skipFolders",False) == False and nodeType==0 or nodeType !=0 : 
				raise OTNodeProcessError(f"No versions found for {nodeId} of type {nodeTypeName}")
			else :
				return False
				
		if ver["version_number_minor"] !=0:
			verNum = ver['version_number']
			

			
			url = self.url_base +f"api/v2/nodes/{nodeId}/versions/{verNum}/promote"
			
			self.log.debug(f"promoring {nodeId} ver {verNum} \n{url}")

			res  = requests.post(
				url =url, 
				headers = { "otcsticket":self.ticket["code"]},
			) 
			return res
		else:
			return False       
		

	def moveNode(self,**kargs):
		#defults
		nodeId = kargs.get("nodeId",self.root_id)
		try:
			parentId = kargs["parentId"]
		except KeyError:
			raise OTRequestError("Parent node not provided")
		
		url  = self.url_base +f"api/v2/nodes/{nodeId}"
		data = {"parent_id": parentId }

		ot_data = self.resolveResponse( 
			requests.put(
				url = url,
				headers = { "otcsticket":self.ticket["code"]},
				data={"body": json.dumps(data)  }
		))
		
		return ot_data
		 
		

#-----------------------------------------------------------------------
#Tests
#-----------------------------------------------------------------------
	def validateNodeMetaData(self,nodeId,metadata):
		catVals  = []
		catData = self.getCategoryValues(nodeId)
		for catId in catData :
			catVals+= [k for k in catData[catId]]
		try:
			for k in metadata:
				if catVals[k] != metdata[k]:
					self.log.debug(f"value error in metdata validation \nnodeId:{nodeId}\nk={k}\n metadata: {metadata[k]}\ncatVals=[catVales[k]]")
					return False
		except KeyError:
			self.log.debug(f"key error in metdata validation \nnodeId:{nodeId}\nk={k}")
			return False


		return True
			





#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#Utils
#-----------------------------------------------------------------------
	def resolveResponse(self,res=None,data=None,error_ok=False):
		try:
			
			data = data or res.json()
			if data.get("error") and not error_ok:
				raise OTRequestError( data["error"] )
			else:
				return data
							
		except json.JSONDecodeError as err:
			
			self.log.critical("Unable to resolve the OT request")
			self.log.critical(res)
			self.log.critical(res.reason)
			
			raise TypeError( res.reason )


	def allPages(self,res):		
		

		res  = res.json()
		if res.get("error"):
			return res
		data = res["results"]

		try:
			href = res["collection"]["paging"]["links"]["data"].get("next")
		except KeyError as err:
			href = False
	
		while href:
			res = requests.get(			
				url = self.url_base+href["href"],
				headers = { "otcsticket":self.ticket["code"]},		    
				).json()
			
			print("page {} of {}".format(res["collection"]["paging"]["page"], res["collection"]["paging"]["page_total"] ))
			
			href =res["collection"]["paging"]["links"]["data"].get("next",False)
			data +=res["results"]
		res["results"] = data
		return res
		


