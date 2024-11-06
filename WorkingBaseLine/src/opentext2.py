from pprint import pprint
import json
import urllib.parse
import re
import csv
import os
import datetime,time
import code
import IPython
import requests
import keyring



if __name__ != "__main__":
    from src.core import Core, Central
else:
    from core import Core, Central
    

# ~ URL_BASE = "http://svrvmsa1362.elec.eskom.co.za/OTCS/cs.exe/"
URL_BASE = "http://opentextcs.eskom.co.za/otcs/cs.exe/"



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
#Decorators
#-----------------------------------------------------------------------
  
def all_pages(f):        
    def wrapper(*args,**kargs):       
        
        res  =  f(*args,**kargs)        
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

#-----------------------------------------------------------------------
#Exceptions
#-----------------------------------------------------------------------
  
class OTGeneralError(Exception):
    pass

class OTNodeProcessError(Exception):
    pass

    
class OTSessionExpiredError(Exception):
    pass
  
class OTExistingNodeNotFoundError(Exception):
    pass  




class Opentext(Core):
    
    def __init__(self,**kargs):
        super().__init__(**kargs)        
        self.station = kargs["station"]
        
        if self.station == "KOEBERG":
            #self.node_tree = {"root":{"_id":1536814}}
            # ~ self.node_tree = {"root":{"_id":1548605 }}
            # ~ self.node_tree = {"root":{"_id":154d9684  }}
            # ~ self.node_tree = {"root":{"_id":1683279   }}
            self.node_tree = {"root":{"_id":1727260    }}
        elif self.station == "ARNOT":
            self.node_tree = {"root":{"_id":1530864}}
        elif self.station == "HYPERWAVE":
            self.node_tree = {"root":{"_id":8327297}}
        elif self.station == "SGR":
            self.node_tree = {"root":{"_id":11968443 }}
        
        
            
        self.username =kargs["username"]    
        
        self.ticket = None
        # ~ self.ot_ticket()
        
        self.cur_ver_maj =0
        
        self.source_data_system = kargs.get("source_data_system","pigo_001")
        
    
#-----------------------------------------------------------------------
#MAIN
#-----------------------------------------------------------------------
    def process(self,node):
       
        self.ot_ticket()
        self.log.debug("*"*50)
        parentId = self.mkpath(node[0])           
        self.cur_ver_maj = 0 
        file_created =  False        
        
        ex = 0
        
        self.log.info("Version Loop")
        for ver in node:            
            self.ot_ticket()            
            time_out = 5
            
            while True:
                try:                  
                    self.convert_metadata( ver )                                
                    self.JD(self.node_tree,"output","examples","tree.json")
                    self.JD(ver,"output","examples","ver.json",default=json_default)                    
                    
                    #add dummies
                    self.log.debug(f'current version {self.cur_ver_maj}  desired version {ver["versions"]["major"]}')                       
                    #---------------------------------------------------
                    if  self.cur_ver_maj >  ver["versions"]["major"]:
                        self.JD(node,"output","examples","node_error.json", default=json_default)
                        raise ValueError("Desired version is less than current")            
                    #---------------------------------------------------
                    
                    
                    while self.cur_ver_maj < ver["versions"]["major"]-1:                        
                        
                                                
                        if not file_created:
                            
                            dummy_node = self.mkDummy(maj_ver = self.cur_ver_maj)                            
                            dummy_node["filename"] = ver["filename"]
                            node_id = self.ot_createFile(parentId,dummy_node)                            
                            file_created = True
                        else:
                            dummy_node = self.mkDummy(maj_ver = self.cur_ver_maj+1)
                            self.log.debug("Adding dummy levels")
                            self.ot_addVersion(dummy_node,node_id)                
                            
                        
                        #self.cur_ver_maj+=1            
                    
                    self.log.debug(f' after dummies current version {self.cur_ver_maj}  desired version {ver["versions"]["major"]}')                    
                    
                    
                    #---------------------------------------------------
                    if  self.cur_ver_maj >  ver["versions"]["major"]:
                        self.JD(node,"output","examples","node_error.json", default=json_default)
                        raise ValueError("Desired version is less than current")            
                    #---------------------------------------------------
                    
                    #add data
                    if not file_created:   
                        node_id = self.ot_createFile(parentId,ver)
                        file_created = True
                    else:
                        self.ot_addVersion(ver,node_id)
                    
                    self.ot_addMetaData(node_id,ver["roles"])
                    
                    ex = 0
                    break
                
                
                
                
                except requests.exceptions.ReadTimeout as err:                    
                    self.log.info(f'timeout sleeping for {time_out}' )
                    time.sleep(time_out)                    
                    
                    if time_out < 200:                                                
                        time_out *= 2
                        
                    self.ot_ticket()  
                    
                except OTNodeProcessError as err:
                    if err.args[0] == "Session Expired":
                        if ex < 20:
                            self.ot_ticket()    
                            self.log.debug(self.ticket)
                            ex+=1
                        else:
                            raise OTSessionExpiredError("Session renew limit exeeded")
                    else:
                        raise err
 

            
    
    
#-----------------------------------------------------------------------
#REST ACTIONS
#-----------------------------------------------------------------------
    

#--------------------------------------------
#Test credentials
#--------------------------------------------

#--------------------------------------------
        
    def ot_ticket(self):
        #self.log.info('Hello 123')
        if self.ticket == None or self.ticket["expire"] <  datetime.datetime.now():            
            
            
            pswrd = 'Esk0m@1234'  #= keyring.get_password(self.username,"password" )    
            username = 'Admin'
            
            data = requests.post(
                URL_BASE+"api/v1/auth",
                data = [("username", username), ("password",pswrd) ]
            )
            
            try:
                data = data.json()
            except json.JSONDecodeError as err:
                print(data)
                print(data.reason)
                raise err
                
            
            try:
                raise OTSessionExpiredError( data["error"] )
            except KeyError:
                pass
                
                
            self.log.debug("ticket_updated")
            exp = datetime.datetime.now()+datetime.timedelta(minutes=10)
            self.ticket = {"code":data["ticket"],"expire":exp}
        #IPython.embed()
        
    def ot_findSubNode(self,parentId,name,versions=False):
        
        url_name = urllib.parse.quote(name)
        url = URL_BASE+"api/v2/nodes/{}/nodes?where_name={}".format(parentId,url_name)
            
        ot_data = requests.get(
            url,
            headers = { "otcsticket":self.ticket["code"]},  
            
        ).json()
        
        
        for node in ot_data["results"]:
            node = node["data"]["properties"]
            if node["name"] == name:
                return node["id"]
        
        raise OTGeneralError("Node not found")
        
        


    #@all_pages  may intorduce error if large search results
    def ot_findSubNodeVersions(self,parentId,name):        
        
        
        name = name.strip()
        fl_search_counter = 0
        while fl_search_counter < 4 :                    
        
            num_char_to_use = int( len( name ) / (2*fl_search_counter or 1 ) )
            
            search_name = name[:num_char_to_use]
            url = URL_BASE+"api/v2/nodes/{}/nodes?".format(parentId)
            url+="where_name={}".format(name)        
            url+= "&fields=versions&fields=properties{id,name}"
        
            
            search_data = requests.get(
                url,
                headers = { "otcsticket":self.ticket["code"]},                  
            ).json()
                        
            for search_node in search_data["results"]:                
                if search_node["data"]["properties"]["name"] == name:
                    return search_node
                    
            fl_search_counter+=1
        
        
        return None
        

    def ot_createFolder(self, parentId,name):
        
        try:
            ot_data = requests.post(
                url = URL_BASE+"api/v2/nodes/",
                headers = { "otcsticket":self.ticket["code"]},            
                data = {"body":json.dumps({
                            "type":0,
                            "name":name,
                            "parent_id":parentId,
                            "versions_control_advanced":True,
                            "advanced_versioning":True
                    })}            
                )
            ot_data = ot_data.json()
        except json.JSONDecodeError as err :
            self.log.error("could not create folder")
            breakpoint()
            
        #pprint(ot_data)
        
        try:
            node_id = ot_data["results"]["data"]["properties"]["id"]
        except KeyError:                        
            if re.match("An item with the name '.+' already exists",ot_data["error"]):
                #item exists get id                
                node_id = self.ot_findSubNode(parentId,name)
                self.log.debug(f"name:{name} parentId:{parentId} node_id:{node_id}")
                
            else:                
                raise OTNodeProcessError(ot_data["error"])
        try:
            res = requests.put(url = URL_BASE+"api/v2/nodes/{}/".format(node_id),
                headers = { "otcsticket": self.ticket["code"]},
                data = {"body":json.dumps({ "advanced_versioning":True,})}            
            )
        except Exception as err:            
            self.log.error(f"could not add advance versioning folder {err}")
            
        return node_id 
        
     
    
    def ot_createFile(self,parentId,node):        
        self.log.debug("CREATE FILE")
        
        
        ot_data = requests.post(
            url = URL_BASE+"api/v2/nodes/",
            headers = { "otcsticket":self.ticket["code"]},            
            data = {"body":json.dumps({
                    "type":144,
                    "name": node["filename"],
                    "parent_id":parentId, 
                    "advanced_versioning":True,                   
                    "add_major_version": node["versions"]["major"] != self.cur_ver_maj,                                       
                    
                    "roles":{"catagories":node["roles"]}                    
                    },default = json_default)},
                    
            files= {"file":(node["filename"], self.filedataToBin(node["filedata"])  )},
        ).json()
        
        self.JD(ot_data,"output","examples","file_data.json")
        
        try:
            node_id = ot_data["data"]["properties"]["id"]               
            #-----------------------------------------------------------                        
            self.log.debug(" RES> id:{: <30}, cur_ver_maj:{: <30}, node_maj:{: <30}".format( node_id,self.cur_ver_maj,  node["versions"]["major"]  ))
            #-----------------------------------------------------------
            #self.cur_ver_maj+=1   
            
            
        except KeyError:            
            if re.match("An item with the name '.+' already exists.",ot_data["error"]):                
                self.log.debug("Key error resetting versions")
                
                
                #file already exists delete all versions except the first to ensure corret upload
                ot_data = self.ot_findSubNodeVersions(parentId,node["filename"])[0]["data"]                 
                node_id = ot_data["properties"]["id"]                 
                
                
                for v in ot_data["versions"][1:]:
                    self.ot_deleteVersion(node_id,v["version_number"])                    
                
                #add first version     
                self.ot_addVersion(node,node_id,1)
                 
                
            else:
                raise OTNodeProcessError( ot_data["error"] )
        
        return node_id
    
    def ot_addVersion(self,node,otId,version_nr=None):        
        
        self.log.info("ot_addVersion to {}".format(otId))
        url = URL_BASE+"api/v2/nodes/{}/versions/".format(otId)
        
        if version_nr:
            fnk = requests.put
            url+=str(version_nr)+"/"
        else:
            fnk = requests.post
        
        ot_data = fnk(
            url = url,
            headers = { "otcsticket":self.ticket["code"]},            
            data = {"body":json.dumps({                   
                        "name": node["filename"],                                       
                        "add_major_version": node["versions"]["major"] > self.cur_ver_maj
                        })},
                        
            files= {"file":(node["filename"], self.filedataToBin(node["filedata"]) )},
        ).json()
        
        try:
            raise OTNodeProcessError( ot_data["error"])
        except KeyError:
            pass
      
    
        #-----------------------------------------------------------
        node_id = ot_data["results"]["data"]["properties"]["id"]   
        node_ver_maj = ot_data["results"]["data"]["versions"]["version_number_major"]
        node_ver_min = ot_data["results"]["data"]["versions"]["version_number_minor"]            
        self.log.debug(" RES>: cur_ver_maj: {: <5}, node_req_ver:{: <5}   ,node_ver_maj_ot:{: <5}, ot_min_ver:{}".format(
            self.cur_ver_maj, 
            node["versions"]["major"],
            node_ver_maj,
            node_ver_min
        ))
        #-----------------------------------------------------------
                
        if node["versions"]["major"] > self.cur_ver_maj:
            self.cur_ver_maj+=1
        
        
        self.JD(ot_data,"output","examples","ver_data.json")
            
                
   
    def ot_deleteVersion(self,nodeId,version):
        res = requests.delete(
            url = URL_BASE+"api/v2/nodes/{}/versions/{}".format(nodeId, version),
            headers = { "otcsticket":self.ticket["code"]}
        ).json()
            
        try:
            raise OTNodeProcessError(res["error"])
        except KeyError:
            pass
        
        
              

    

    
    @all_pages
    def ot_getSubnodes(self,nodeId):
        res = requests.get(            
            url = URL_BASE+"api/v2/nodes/{}/nodes/".format(nodeId),
            headers = { "otcsticket":self.ticket["code"]},            
        ).json()
        try:
            raise OTNodeProcessError(res["error"])
        except KeyError:
            return res
        

    def ot_treeDelete(self,nodeId,n=0,folders=False):
        
        #getSubnodes
        nodes = self.ot_getSubnodes(nodeId)

        for node in nodes:
            if node["data"]["properties"]["type"] ==0:
                self.ot_treeDelete(node["data"]["properties"]["id"],n+1)
                
                if n!=0 and folders:
                    ot_data = requests.delete(
                        url = URL_BASE+"api/v2/nodes/{}".format(nodeId),
                        headers = { "otcsticket":self.ticket["code"]},            
                    ).json()
                    
                    if ot_data.get("error"):
                        raise OTGeneralError(ot_data.get("error"))
                
            elif node["data"]["properties"]["type"] ==144:
                ot_data = requests.delete(
                    url = URL_BASE+"api/v2/nodes/{}".format(node["data"]["properties"]["id"]),
                    headers = { "otcsticket":self.ticket["code"]},            
                ).json()
                
                if ot_data.get("error"):
                    raise OTGeneralError(ot_data.get("error"))
            
            else:
                pprint( node  )
                return
                

          
        
    def ot_addMetaData(self,nodeId,roles):
        
        url=URL_BASE+"api/v2/nodes/{}/categories/1531395".format(nodeId)    
        roles["category_id"]=1531395
        try:
            err = requests.put(url,        
                data= roles,
                headers = { "otcsticket":self.ticket["code"]},
            ).json()["error"]
            raise OTNodeProcessError(err)        
        except KeyError:
            pass
     
    @all_pages 
    def ot_subnodeVersions(self,nodeId):        
        
        url = URL_BASE+"api/v2/nodes/{}/nodes".format(nodeId)        
        url+="?fields=properties{id,name,type,type_name}"                     
        url+= "&fields=versions{file_name,version_number,version_number_major,version_number_minor}"
        
        
        res = requests.get(            
            url = url,
            headers = { "otcsticket":self.ticket["code"]},            
        ).json()
        
        
        
        try:
            raise OTGeneralError(res["error"])
        except KeyError:
            return res
        
        
        
#-----------------------------------------------------------------------
#SUPPORT ACTIONS
#-----------------------------------------------------------------------

        
    
        
        
    
    def mkpath(self, ver):
        
        path = [                
            "root",
            ver["meta"]["CATVER_CAT_TYP"] or "None",
            ver["meta"]["CATVER_DOC_TYP"] or "None",
            ver["filename"]
        ]
        
        tree = self.node_tree
        for n in path[:-1]:
            try:
                tree = tree[n]
            except KeyError:
                tree[n] = {"_id":self.ot_createFolder(tree["_id"],n)}
                tree = tree[n]
            
         
         
        # ~ n = path[-1]
        # ~ try:
            # ~ tree = tree[n]
        # ~ except KeyError:
            # ~ tree[n] = {"_id":self.ot_createFile(tree["_id"],ver)}
            # ~ tree = tree[n]
            
        
        return tree["_id"]
    
    
    def convert_metadata(self,ver, ):
        if self.source_data_system == "pigo_001":
            
            self.metadata_pigo_001(ver)
        
    def metadata_pigo_001(self,ver):        
        ver["roles"] ={
                        "1531395_10": ver["meta"]["CATVER_CAT_REF"],
                        "1531395_11": ver["meta"]["CATVER_DES"],
                        "1531395_12": ver["meta"]["CATVER_DOC_STA"],
                        "1531395_13": ver["meta"]["CATVER_DAT_CRT"],
                        "1531395_14": ver["meta"]["CATVER_ARC_DAT"],
                        "1531395_15": ver["meta"]["CATVER_REC_ID"],
                        "1531395_16": ver["meta"]["CATVER_SRY_CLS"],
                        "1531395_17": ver["meta"]["CATVER_COM"],
                        "1531395_18": ver["meta"]["CATVER_ALA_DAT"],
                        "1531395_19": ver["meta"]["CATVER_LAST_UPD_BY"],
                        "1531395_2": ver["meta"]["CATVER_FUNC"],
                        "1531395_20": ver["meta"]["CATVER_REV_DAT"],
                        "1531395_21": ver["meta"]["CATVER_STAT_COM"],
                        "1531395_22": ver["meta"]["CATVER_NO_OF_SHT"],
                        "1531395_23": ver["meta"]["CATVER_COM_REF"],
                        "1531395_24": ver["meta"]["CATVER_DOC_REF_DAT"],
                        "1531395_3": ver["meta"]["CATVER_DAT_APP"],
                        "1531395_4": ver["meta"]["CATVER_ORG_LOC"],
                        "1531395_5": ver["meta"]["CATVER_DOC_TYP"],
                        "1531395_6": ver["meta"]["CATVER_CUR_REV"],
                        "1531395_7": ver["meta"]["CATVER_CAT_TYP"],
                        "1531395_8": ver["meta"]["CATVER_VER"],
                        "1531395_9": ver["meta"]["CATVER_PAG"],
                        
                        
                        # ~ "1531395_25": ver["meta"]["CATVER_COM_REF"],
                        # ~ "1531395_26": ver["meta"]["CATVER_DOC_REF_DAT"],
                        # ~ "1531395_27": ver["meta"]["CATVER_DOC_REF_DAT"],
                        
                        


                    }                   
    
    def mkDummy(self,**kargs):
        return {
            "filename":"dummy.txt",
            "filedata":b'version placeholder',                
            "roles":{},
             "versions": {
                "major": kargs["maj_ver"],                
                },
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

        
#-----------------------------------------------------------------------
#INFO
#-----------------------------------------------------------------------

    def mknodeTreeCSV(self,nodeID,path=None,n=0,w=None,fl_csv=None):        
        self.ot_ticket()
        if w==None:
            self.fl_csv = open(f"output/ot/node_tree_{nodeID}.csv","wt",newline="\n")
            w = csv.writer(self.fl_csv)
            w.writerow([
                "id","name",
                # ~ "version_number",
                # ~ "version_number_major",
                # ~ "version_number_minor",
                "file_name","path"
            ])
        
        
        if path==None:
            node = self.ot_nodeInfo(nodeID)
            path = node['results']['data']["properties"]["name"]
            
        
        
        self.log.info(path)
        res  = self.ot_subnodeVersions(nodeID)
        
        for node in res:
            #subnodes.append( node["data"] )            
            
            if node["data"]["properties"]["type"]==0:
                node["data"]["subnodes"] = []
                try:
                    
                    self.mknodeTreeCSV(
                        node["data"]["properties"]["id"],
                            path+"/"+node["data"]["properties"]["name"],
                            n+1,w)
                    
                except Exception as err:
                    self.log.error(err)
            else:
                if node["data"]["versions"]:                
                    for ver in  node["data"]["versions"]:
                        w.writerow([
                            node["data"]["properties"]["id"],
                            node["data"]["properties"]["name"],
                            # ~ ver["version_number"],
                            # ~ ver["version_number_major"],
                            # ~ ver["version_number_minor"],
                            ver["file_name"],
                            path
                            ])
                else:
                    w.writerow([
                            node["data"]["properties"]["id"],
                            node["data"]["properties"]["name"],
                            -1,
                            -1,
                            -1,
                            node["data"]["properties"]["name"],
                            path
                            ])
                    
            
        if n==0:            
            #self.JD({"nodes":subnodes},"output","examples","subnodesversions.json")
            self.fl_csv.close()
        
    
        
#-----------------------------------------------------------------------
#DEBUG
#-----------------------------------------------------------------------
    
    
    def ot_AddPigoCat(self,cat=1531395,root=None):
             #add category to folder
        root = root or self.node_tree["root"]["_id"]
        
        res = requests.post(url = URL_BASE+"api/v2/nodes/{}/categories".format(root),
            headers = { "otcsticket":self.ticket["code"]},
            data = {"body":json.dumps({
                "category_id":cat
                              
        })})
        try:
            err = res.json()["error"]
            raise OTGeneralError(err)
        except KeyError:
            pass
        
        
        #set inheritance 
        res = requests.post(
            url = URL_BASE+"api/v2/nodes/{}/categories/1531395/inheritance".format(root),
            headers = { "otcsticket":self.ticket["code"]},
            data = {"body":json.dumps({ "category_id":cat})})
        try:
            err = res.json()["error"]
            raise OTGeneralError(err)
        except KeyError:
            pass
    
        
        
    
    
    def gogo(self):          
        pass
        
        res = requests.post(
            url = URL_BASE+"api/v1/nodes/13074271/categories/556452/inheritance ",
            headers = { "otcsticket":self.ticket["code"]},            
        )
        print(res.reason)
        
        # ~ self.ot_treeDelete(13074271)
        #node_id = self.ot_createFolder(5730593,"NOU MIGRATION")        
        
        
        
        code.interact(local=locals())
        
        
        
        # ~ pprint(ot_data)
        
        # ~ code.InteractiveConsole(locals=locals())
        # ~ breakpoint()
       
       
class OpentextInfo(Opentext):
    
    
    def __init__(self,**kargs):
        kargs["station"] = kargs.get("station","KOEBERG")
        super().__init__(**kargs)        
        self.url_base = kargs.get("url_base",URL_BASE)
        
        
        
    
    def ot_ticket(self):
        
        if self.ticket == None or self.ticket["expire"] <  datetime.datetime.now():                        
            pswrd = keyring.get_password(self.username,"password" )    
            
            data = requests.post(
                self.url_base+"api/v1/auth",
                data = [("username", self.username.split("@")[0]), ("password",pswrd) ]
            )
            
            try:
                data = data.json()
            except json.JSONDecodeError as err:
                print(data)
                print(data.reason)
                raise err
                
            try:
                raise OTSessionExpiredError( data["error"] )
            except KeyError:
                pass
                
                
            self.log.debug("ticket_updated")
            exp = datetime.datetime.now()+datetime.timedelta(minutes=10)
            self.ticket = {"code":data["ticket"],"expire":exp}
    
    
    def ot_nodeInfo(self, nodeID,fields=None):        
        self.ot_ticket()
        url= self.url_base+"api/v2/nodes/{}".format(nodeID)
        
        
        if type(fields) == str:
            url+=f"?fields={fields}"
            
        elif type(fields) == list:
            fields = "&".join( [f"fields={f}" for f in fields] )
            url+=f"?{fields}"
                
        
        res = requests.get( 
            url= url,
            headers = { "otcsticket":self.ticket["code"]},        
        )
        
        try:
            res = res.json()
        except Exception as err:
            
            for msg in [res.reason,res.text]:
                try:                
                    self.log.critical(msg)
                except:
                    pass
            raise OTGeneralError(err.args[0])
        
        try:
            raise OTGeneralError(res["error"])
        except KeyError:
            return res        
    
    
    def gogo(self):
        self.ot_ticket()
        
        folder_id = 13333996
        folder_id = 1531832
        
        res = requests.post(
            url = self.url_base+"api/v2/nodes/",
            headers = { "otcsticket":self.ticket["code"]},
            data={  "body": json.dumps({
                "parent_id":folder_id,
                "name":"kbp_custom_cat_01",
                "type":131
            })
            }
        )
        
        return res

class HWOpenText(Opentext):
    def __init__(self,**kargs):
        kargs["station"] = "HYPERWAVE"
        kargs["username"] = "s-hw_mig_opentext@opentext"        
        super().__init__(**kargs)
        
        #from src.opentext2 import HWOpenText
        
    
    
    def ot_createFile(self,parentId,node):        
        self.log.debug("CREATE FILE")
        
        
        ot_data = requests.post(
            url = URL_BASE+"api/v2/nodes/",
            headers = { "otcsticket":self.ticket["code"]},            
            data = {"body":json.dumps({
                    "type":144,
                    "name": node["filename"],
                    "parent_id":parentId, 
                    "advanced_versioning":True,                   
                    "add_major_version": node["major"],
                    "roles":{"catagories":node["roles"]}                    
                    },default = json_default)},                    
            files= {"file":(node["filename"], self.filedataToBin(node["filedata"])  )},
        ).json()
        
        try:
            node_id = ot_data["results"]["data"]["properties"]["id"]               
            
            if node["major"]:
                res  = requests.post(
                url = URL_BASE+f"api/v2/nodes/{node_id}/versions/1/promote",
                headers = { "otcsticket":self.ticket["code"]},
                )
                
            #-----------------------------------------------------------                        
            #self.log.debug(" RES> id:{: <30}, cur_ver_maj:{: <30}, node_maj:{: <30}".format( node_id,self.cur_ver_maj,  node["versions"]["major"]  ))
            #-----------------------------------------------------------
            #self.cur_ver_maj+=1   
            
        except KeyError:            
            err_text = ot_data.get("error",False)
            
            if err_text and re.match("An item with the name '.+' already exists.", err_text):                
                self.log.debug("File already exists resetting versions")
                
                
                
                #file already exists delete all versions except the first to ensure corret upload
                
                #find correct node
                try:
                    ot_data = self.ot_findSubNodeVersions(parentId, node['filename'])["data"]                
                except TypeError:
                    raise OTExistingNodeNotFoundError(f"node ({ node['filename']}) not found in {parentId}")
                    
                
                if ot_data == None:                
                    raise OTExistingNodeNotFoundError(f"Could not find '{ node['filename']}'")
                
                node_id = ot_data["properties"]["id"]
                
                
                for v in ot_data["versions"][1:]:
                    self.ot_deleteVersion(node_id,v["version_number"])                    
                
                #add first version     
                self.ot_addVersion(node,node_id,1)
                
                    
                 
                
            else:
                self.log.error("error while deleteing verions")
                
                raise OTNodeProcessError( json.dumps(ot_data) )
        
        return node_id
    
    def ot_addVersion(self,node,nodeId,versionNr=None):        
        
        self.log.info("ot_addVersion to {} ({})".format(nodeId,node["major"]))
        url = URL_BASE+"api/v2/nodes/{}/versions/".format(nodeId)
        
        if versionNr:
            fnk = requests.put
            url+=str(versionNr)+"/"
        else:
            fnk = requests.post
        
       
        ot_data = fnk(
            url = url,
            headers = { "otcsticket":self.ticket["code"]},            
            data = {"body":json.dumps({                   
                        "name": node["filename"],                                       
                        "add_major_version": node["major"]
                        })},
                        
            files= {"file":(node["filename"], self.filedataToBin(node["filedata"]) )},
        ).json()
        
        
        
        if ot_data.get("error"):    
            msg = ot_data.get("error")            
            
            if versionNr == 1:
                try:    
                    url = URL_BASE+"api/v2/nodes/{}/versions/".format(nodeId)
                    ot_data = requests.post(
                        url = url,
                        headers = { "otcsticket":self.ticket["code"]},            
                        data = {"body":json.dumps({                   
                            "name": node["filename"],                                       
                            "add_major_version": node["major"]
                            })},
                            
                        files= {"file":(node["filename"], self.filedataToBin(node["filedata"]) )},
                    ).json()    
                    
                except Exception as err:
                    self.log.error("{err.args[0]} cause file to fail (retried version add)")
                    raise err
            else:                
                raise OTNodeProcessError( msg )
        
        try:
            ver = ot_data['results']['data']['versions']
        except KeyError:
            breakpoint() 
        
        if ver['version_number']==1 and  ver['version_number_major']==0 and node["major"]:
            
            #premote version has a bug. Work around
            # ~ res  = requests.post(
                # ~ url = URL_BASE+f"api/v1/nodes/{nodeId}/versions/1/promote",
                # ~ headers = { "otcsticket":self.ticket["code"]},
                # ~ )
                
            # ~ self.log.debug(f"{res.status_code}")            
            self.ot_addVersion(node,nodeId,versionNr=None)            
            self.ot_deleteVersion(nodeId,version=1)
            
            
            
            
        
        
    
    def ot_addMetaData(self,nodeId,roles,catId=1531395):
        self.log.debug(f"adding metadata {catId} to {nodeId} ")
        
        url=URL_BASE+"api/v2/nodes/{}/categories/{}".format(nodeId,catId)    
        roles["category_id"]=catId
        
        
        while True:            
            err = requests.put(url,        
                data= roles,
                headers = { "otcsticket":self.ticket["code"]},
            ).json().get("error",False)            
            
            if not err:
                break            
            
            roles_to_del = []
            if err == "The value is not one of the valid values.":
                for r in roles:
                    try:
                        if  roles[r] not in self.cen.meta["cat_values"][r]:
                            self.log.error(f"{r} with value {roles[r]} not allowed. Removing value")
                            roles_to_del.append(r)
                            
                    except KeyError:
                        pass
                        
                for r in roles_to_del:
                    del roles[r]
                self.log.error(f"Values after cleanup {roles}")
            else:
                
                self.log.critical(json.dumps(roles,indent=4))
                self.log.critical(err)
                raise OTNodeProcessError(err)        
                

    
    def ot_nodeInfo(self, nodeId):        
        res = requests.get( 
            url= URL_BASE+"api/v2/nodes/{}".format(nodeId),
            headers = { "otcsticket":self.ticket["code"]},        
        )
        
        try:
            res = res.json()
        except Excpetion as err:
            raise OTGeneralError(err.args[0])
        
        try:
            raise OTGeneralError(res["error"])
        except KeyError:
            return res     


