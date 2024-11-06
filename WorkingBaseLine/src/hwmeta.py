from pprint import pprint
import os
import io
import hashlib
import bson
import xml.etree.ElementTree as ET
import re
import datetime
import csv,json
import code


import mimetypes 
import requests

from src import core
from src import opentext2




#-----------------------------------------------------------------------
#Errors
#-----------------------------------------------------------------------
class HWCRepsonseContentError(Exception):
    pass


#-----------------------------------------------------------------------
#Decorators functions 
#-----------------------------------------------------------------------



class HWMigrator(core.Core):
    
    def __init__(self,**kargs):
        super().__init__(**kargs)        
        
        self.N = kargs.get("N",-1)
        
        #interacts with hyperwave (soap requests)
        self.hw = HWClient(cen=self.cen)        
        self.hw.getSessionID()
        
        
        #interacts with OpentText (rest requests)
        self.ot = opentext2.HWOpenText(cen=self.cen)
        #tracks the current folder
        self.ot_cur_id = self.ot.node_tree["root"]["_id"]
        
        
        
        #set save file if one is provided 
        self.datafile = kargs.get("datafile",None)
        self.file_count = 0
        self.error_log = {}
        
        
    #-------------------------------------------------------------------
    # UTILS
    #-------------------------------------------------------------------
    
    def strToDate(self,d):
        if d==None:
            return None
        
        
        m = re.match("(\d{4})/(\d{2})/(\d{2})$",d) #$ required or the it will match the longer version with times
        if m:
            return datetime.date( int(m.group(1)), int(m.group(2)), int(m.group(3)) )
        m = re.match("^(\d{4})/(\d{2})/(\d{2}) (\d+):(\d+)",d)
        if m:
            return datetime.datetime( int(m.group(1)), 
                                  int(m.group(2)), 
                                  int(m.group(3)), 
                                  int(m.group(4)), 
                                  int(m.group(5)), 
                                )
        raise TypeError(f"Unknown date format {repr(d)}") 
    
    
    
    def update_data(self,backup=True):        
        
        if backup:
            
            dr,fl = os.path.split( self.datafile )
            fl,ext = os.path.splitext( fl)            
            
            dr = os.path.join(dr,"backup",fl)            
            os.makedirs( dr, exist_ok=True )
            
            dt  = datetime.datetime.now().strftime(f"{fl}_%Y%m%d%H%M{ext}")            
            fl = os.path.join( dr, dt )
            
            try:
                self.BW(self.B( self.datafile), fl )
            except FileNotFoundError:
                self.log.warning("No datafile to backup")
            
            fls = sorted( [f for f in os.listdir( dr )],reverse=True )
            for f in  fls[3:]:
                os.remove( os.path.join(dr,f) )
            
            
        self.log.info(f"data update ({self.datafile})")        
        self.JD(self.data,self.datafile)
        self.log.debug(f"  done")        
        
    #-------------------------------------------------------------------
    # PROCESS
    #-------------------------------------------------------------------
    
    def processFromData(self,data,ot_cur_id=None,n=0):
        
        #if this is the root call to the function set the progess data file
        #and other vars
        if n==0:
            self.datafile = self.datafile or  f'output/HW/{data["self"]["HW_OID"]}_data.json'
            self.file_count = 0
            self.data = data
        
        
        
        folder_error = False
        ot_cur_id = ot_cur_id or self.ot.node_tree["root"]["_id"] 
                       
        try:
            self.log.debug(f'{ " "*(3*n) }{len(data["files"])} files {len(data["folders"])} folders')
        except Exception as err:            
            self.log.error(f'{type(err)} {err}')
            raise err
            
        #migrate files
        
        try:
            #if root is a file
            if n==0 and data["self"]["DocumentType"].lower() == "generic":
                data["files"].append(data["self"])
                self.log.info(" "*(3*(n+1))+"root is file")
                
            
            
            files_empty = True
            for i,d in enumerate(data["files"]):
                files_empty = False         
                if d.get("migrated",False) == False:
                    self.log.info(" "*(3*(n+1)) + f"Migrating file {d['HW_OID']} {self.file_count}")                
                    
                    self.ot.ot_ticket()
                    try:
                        self.processHWFile(d,ot_cur_id)                
                        data["files"][i]["migrated"] = True
                    except Exception as err:
                        data["files"][i]["migrated"] = err.args[0]
                        folder_error = True
                    
                    self.file_count+=1
                    if self.file_count > 5:
                        self.update_data()
                        self.file_count = 0                    
                        
            data["self"]["files_empty"] = files_empty
            
            for i,f in enumerate(data["folders"]): 
                if f["self"].get("migrated",False) == True:
                    continue                
                
                self.hw.getSessionID()
                self.ot.ot_ticket()
                ot_node_id = self.ot.ot_createFolder(ot_cur_id, f["self"]["HW_ObjectName"])                        
                self.processFromData(f,ot_node_id,n=n+1)                
                
                if f["self"]["migrated"] == False:
                    folder_error = True
                #subfolders have files?
                data["self"]["files_empty"] =  data["self"]["files_empty"] and f["self"]["files_empty"]
                
            #was the folder migrated?
            if folder_error:
                data["self"]["migrated"] = False
            else:
                data["self"]["migrated"] = True
            
                
        except KeyboardInterrupt as err:
            if n!=0:
                raise err            

        self.update_data()
        return data
        
        
    
    def process(self,hwID,n=0,ot_cur_id = None ):        
        
        
        res = self.hw.getObject(hwID)
        ot_cur_id = ot_cur_id or self.ot.node_tree["root"]["_id"]                
        
        self.log.info(f"Processing {hwID}")
        
        #is item is a folder?
        
        try:
            if res[0]['DocumentType'] == 'collection':
                #process folder
                tmp_cur_id = self.processHWFolder(res,ot_cur_id)
                    
                self.log.info(f"Processing children of {hwID} ot id {tmp_cur_id}")
                for item in self.hw.getChildren(hwID):
                    self.process( item["HW_OID"],n+1,tmp_cur_id)
                
                
            elif res[0]['DocumentType'] == 'Generic':
                self.log.info(f"Processing {hwID} as a file")
                self.ot.ot_ticket()
                
                #process file
                try:                    
                    self.processHWFile(res,ot_cur_id)
                except Exception as err:
                    self.log.error(f"Could not process file ({hwID})\nReason: {err.args[0]}")
            else:
                self.log.error(f"Processing {hwID} as a unknonw")
                self.log.error(f" "*4+f"{res[0]['DocumentType']}")
        except Exception as err:
            try:
                self.JD(res,"output","examples","hw_getobject.json")
            except:
                self.log.error("FAILED to export response")
                breakpoint()
            
            raise err
            
        
        
    def processHWFolder(self,res,ot_parnt_id):
        res = res[0]
        return self.ot.ot_createFolder(ot_parnt_id, res["HW_ObjectName"])
        
        
    
    def processHWFile(self,data,ot_parent_id):         
        
        
        
        if type(data) == dict:            
            hw_id = data["HW_OID"]
            meta,filedata = self.hw.getObject(hw_id)
            self.log.debug(hw_id)
            
            
        elif type(data) == str:
            hw_id = data
            meta,filedata = self.hw.getObject(data)
        else:
            meta,filedata = data
        
        
        if not meta:
            msg = f"No metadata returned from HyperWave for {hw_id} "
            self.log.error(msg)
            raise Exception(msg)
            
            
        
        try:
            node = self.resToNode((meta,filedata))
        except Exception as err:
            self.log.debug(data)
            self.log.debug(meta)
            raise err
        
        
        self.addVersions(node)
        
        #self.JD(node,"output","HW","nodeExample.json")
        
        try:
            #create the file
            #remeber this is a reference. Does not matter as first version is only used once
            node_first = node["versions"][0]
            #set node filename to last file name in case its a dummy
            node_first["filename"] = node_first["filename"] == "dummy.txt" and   node_first["filename"] or  node["filename"]                
            
            
            node_id = self.ot.ot_createFile(ot_parent_id, node_first)        
            try:
                data["ot_node_id"] = node_id
            except TypeError:
                pass
            
            self.log.debug("CREATE FILE DONE")
            self.ot.ot_addMetaData(node_id, node_first["roles"],catId=556452)
            
            #add versions
            for ver in node["versions"][1:]:            
                self.ot.ot_addVersion(ver,node_id)        
                self.ot.ot_addMetaData(node_id, ver["roles"],catId=556452)                
            
        except IndexError as err:
            breakpoint()
            raise err
        except opentext2.OTExistingNodeNotFoundError as err:
            self.log.error("could not find existing node")
            self.log.error(err.args)
            raise err
            
            
        except Exception as err:
            self.log.debug(f"parent id:{ot_parent_id}")
            self.log.debug(f"node_first id:{node_first['HW_OID']}")
            
            raise err
        

        
    
    def resToNode(self,res):
        
        
        
        meta = res[0]
        
        for d in ["ReviewDate","TimeCreated","TimeModified","DateApproved"]:
            try:
                meta[d] = self.strToDate( meta.get(d)  ).strftime("%Y-%m-%dT%H:%T+02:00")
            except AttributeError:
                pass
        
        
        try:
            try:
                m = re.match("en *: *(.+)",meta["Title"])
                if m:
                    fl = m.group(1)
                    
                else:
                    fl = meta.get("Title")                    
                    
                title = fl
            except KeyError as err:
                pprint(res[0])
                raise err
                
            
            fln,fl_changes = re.subn("[/\\\\:]","-",fl)
            
            hw_ver = meta.get('HW_Version','0.1').split(".")
            try:
                hw_ver = [int(hw_ver[0]),int(hw_ver[1])]
            except ValueError:
                hw_ver = [0,1]
            
            
            if len(title) > 200:
                self.log.error(f"Title exceeds limit. Limiting: \n {title} => {title[:200]}")
                title = title[:200]
            
            
            hw_checksum = meta.get("HW_Checksum")
            if hw_checksum == None:
                self.log.error(f"Checksum not available:{json.dumps(meta,indent=4)}")
            
            node = {
                "filename": fln,            
                "filedata": res[1],
                "major": False,
                "HW_Version":hw_ver,
                "HW_OID":meta["HW_OID"],            
                "HW_Checksum":hw_checksum,
                "roles":{
            
                        "556452_10" : title,
                        "556452_12" : meta.get("Discipline",meta.get("FunctionalArea")),
                        "556452_13" : meta.get("DMDocAuthor_descr"),
                        "556452_14" : meta.get("DocNumber"),
                        "556452_15" : meta.get("DocOwner_descr"),
                        "556452_16" : meta.get("DocNumber"),                        
                        "556452_17" : meta.get("Discipline",meta.get("FunctionalArea")),                        
                        "556452_19" : meta.get("OriginatingArea"),                    
                        "556452_20" : meta.get("RetentionPeriod"),                    
                        "556452_21" : meta.get("ReviewDate"),
                        "556452_22" : meta.get("TimeCreated"), 
                        "556452_23" : meta.get("TimeModified"),                        
                        "556452_25" : meta.get("Approved_By_descr"),
                        "556452_26" : meta.get("Item_Life_Cycle"),                        
                        "556452_3" : meta.get("SecurityClass"),
                        "556452_30" : meta.get("DocType","NA"),
                        "556452_31" : meta.get("DocSubType"),
                        "556452_32" : meta.get("FunctionalArea"),                        
                        "556452_37" : meta.get("Author"),
                        "556452_4" : meta.get("Status"),
                        "556452_5" : title,
                        "556452_9" : meta.get("DateApproved"),
                        }
            }
            
            roles = {}
            for n in node["roles"]:
                if node["roles"][n]:
                    roles[n]  = node["roles"][n]            
            node["roles"] = roles
            
            
            if fl_changes > 0:
                try:
                    self.error_log["file name error"].append((node,(fl,fln),"REPLACED"))
                except KeyError:
                    self.error_log["file name error"]= [(node,(fl,fln),"REPLACED")]
            
            
            return node
        except KeyError as err:
            self.log.critical("response not acceptable")
            pprint(res[0])
            code.interact(local=locals())
            raise err
        
        
    def addVersions(self,node):
        """Gets each version of the HW file. Files are downloaded and file paths are stored in filedata.
        If a version does not exists a dummy is added.
        """
        
        tmp_dummy_vers = []
        versions = []
        cur_ver_nr = [0,0]
        
        _,oid, ver_max  = node["HW_OID"].split("-")
        ver_max = int( ver_max )
        
        oid = f"{_}-{oid}"
      
            
      
        
        for v in range(ver_max):
            res = self.hw.getObject(f"{oid}-{v+1}")
            if res[0].get("HW_OID",False):
                
                if res[1] ==b'':
                    res = list(res)
                    res[1]=f'No File data found for {res[0]["HW_OID"]}'.encode()
                    
                #-------------------------------------------------------------                    
                #Convert to node
                #-------------------------------------------------------------                    
                node_ver = self.resToNode(res)
                
               
                # ~ #-------------------------------------------------------------                    
                #add version and dummies if needed
                #-------------------------------------------------------------                                    
                
                ver_nr  = node_ver["HW_Version"]
                #versions send as ref. dummy versions appends versions
                self.addDummyVersions(node_ver,cur_ver_nr,versions)
            else:
                self.log.error(f"{oid}-{v+1} no metdata found.")
        
        
        node["versions"] = versions
        
        if len(versions) < 1:
            breakpoint()
        
       
                
                
        
    def addDummyVersions(self,node,cur_ver_nr,versions):
        #get the current node to be added to versions. check if dummys must be added first
        ver_nr = node["HW_Version"]
        
        
        
        while True:
            #need to add next major verion
            if cur_ver_nr[0]+1 == ver_nr[0] and ver_nr[1] ==0 :                    
                # ~ self.log.debug(f"creating major")        
                node["major"] = True
                versions.append(node)
                
                cur_ver_nr[0] += 1
                cur_ver_nr[1]  = 0                        
                break #--------------- end loop ---------------
                
            #need to add dummy major verions
            
            
            elif (cur_ver_nr[0]+1 < ver_nr[0] and ver_nr[1] ==0) or (cur_ver_nr[0] < ver_nr[0] and ver_nr[1] !=0):
                
                
                if len(versions) > 0:
                    dummy_filename =  "dummy.txt"
                else:
                    dummy_filename =  os.path.splitext( node["filename"] )[0]+".txt"
                
                
                versions.append({
                
                    "filename":dummy_filename,                        
                    "filedata":b"version not available",
                    "major":True,   
                    "HW_Version": [cur_ver_nr[0]+1,0],
                    "roles":{}                         
                })
                
                cur_ver_nr[1]  = 0                        
                cur_ver_nr[0]+=1
            
            #need minor version
            elif cur_ver_nr[0] == ver_nr[0] and cur_ver_nr[1]+1 == ver_nr[1]:
                
                node["major"] = False
                versions.append(node)
                cur_ver_nr[1] +=1
                
                
                break  #--------------- end loop ---------------
            #need to make dummy minor verions
            elif cur_ver_nr[0] == ver_nr[0] and cur_ver_nr[1]+1 < ver_nr[1]:
                
                if len(versions) > 0:
                    dummy_filename =  "dummy.txt"
                else:
                    dummy_filename =  os.splitext( node["filename"] )[0]+".txt"
                
                
                
                versions.append({
                    "filename":dummy_filename,                        
                    "filedata":b"version not available",
                    "major":False,     
                    "HW_Version": [cur_ver_nr[0],cur_ver_nr[0]+1],   
                    "roles":{}                                                
                })
                cur_ver_nr[1] +=1
            
            else:
                self.error_log["Version Error"] = self.error_log.get("Version Errors",[])
                self.error_log["Version Error"].append((cur_ver_nr,node))
                break
    
    def __del__(self):
        dt = datetime.datetime.now().strftime("logs/errors_%Y%m%d%H%M.json")
        self.JD(self.error_log,dt)


    
class HWClient(core.Core):
    
    def __init__(self,**kargs):
        super().__init__(**kargs)
        self.url ="http://mp2vlsa046:8082/axis2/services/EskomHWService"
        self.sessionid = None
        
        #-----------------------------------------------------------------------
        #INIT
        #-----------------------------------------------------------------------
        ET.register_namespace("soapenv", "http://www.w3.org/2003/05/soap-envelope")
        ET.register_namespace("esk","http://eskom.hyperwave.com")
        ET.register_namespace("esku","http://util.eskom.hyperwave.com")

        self.prefix_map = {
            "soapenv": "http://www.w3.org/2003/05/soap-envelope",
            "esk":"http://eskom.hyperwave.com",
            "esku":"http://util.eskom.hyperwave.com"
            }

    
    def processResponse(self,res):
        
        data = res.content
        # ~ self.BW(data.encode(),"output","xml","msg.txt")
        
        parts  = re.split(b"--MIMEBoundary.+",data)
        resp = {
            "xml" :[],
            "data" : []            
        }
        
        for i,part in enumerate(parts[1:]):
            try:
                n = part.index(b"\r\n\r\n")              
            except  ValueError as err:               
                continue
                
            meta = part[:n].decode()
            datapart = part[n+4:]            
            m = re.search("Content-Type:(.+)", meta)
            
            if  m.group(1) == ' application/xop+xml; charset=UTF-8; type="application/soap+xml"\r':
                resp["xml"].append( datapart.decode() )
            else:                
                resp["data"].append( datapart[:-2])
                
        
        return resp
    
        
        
        
    def tree(self,res):
        root = ET.fromstring(res)        
        tres = ET.ElementTree( root )
        ET.indent(tres)        
        
        print(ET.tostring(root).decode())
        
        

    def genRequest(self,xml,pat=None):        
        res = requests.post(
            url = self.url,
            data = xml        
            )                  
        return self.processResponse(res)
        
        

    def getSessionID(self,force=False):
        
        try:
            last_req = (datetime.datetime.now() - self.skey_time).seconds
        except AttributeError:
            last_req  = 301
        
        if force or last_req > 300:
            xml = self.T("meta","hwxml","hw_identify.xml")
            pat = "<ns2:SessionID>(.+)</ns2:SessionID>"
        
            self.skey_time  = datetime.datetime.now()
            resp = self.genRequest(xml)
            m = re.search(pat,resp["xml"][0])            
            if m:
                self.sessionid = m.group(1)
            else:  
                self.log.critical(resp)
                raise Exception("Could not get HW Session key")
                
        
        return self.sessionid 
        
        
    def getObject(self,oid,version=None):
        if version != None:        
            xml = self.T("meta","hwxml","hw_object.xml").format(
                SessionID = self.sessionid,
                ObjectID = oid,
                VersionNumber = version
            )
        else:
            xml = self.T("meta","hwxml","hw_meta.xml").format(
                SessionID = self.sessionid,
                ObjectID = oid
            )
        
        
        res = self.genRequest(xml)
        
        
        root = ET.fromstring(res["xml"][0])
        #---------------------------------------------------------------
        tres = ET.ElementTree( root )
        ET.indent(tres)        
        tres.write("output/examples/hwmeta.xml")
        #---------------------------------------------------------------
        
        # ~ self.TW(ET.tostring(tres),"output/examples/hwmeta.xml")
        
        ent = {}
        
        for item in root.findall(".//esku:Object/esku:item",self.prefix_map):
            ent[ item[0].text ] = item[2].text
        
        
        return  ent, res["data"] and res["data"][0]
        
        
    def getChildren(self,oid,debug=False):
        
        xml = self.T("meta","hwxml","hw_getchildren.xml").format(
            SessionID = self.sessionid,
            ObjectID = oid
        )
        res = self.genRequest(xml)["xml"][0]
        if debug:
            print(res)
        
        
        root = ET.fromstring(res)
        tres = ET.ElementTree( root )
        ET.indent(tres)        
        tres.write("output/examples/hwchild.xml")
        
       

        
        docs = []
        for chld in root.findall(".//esk:Objects/esku:item",self.prefix_map):
            ent = {}
            for item in chld:
                ent[item[0].text] = item[2].text
            docs.append(ent)
        
        return docs
        
        
    # ~ def getItem(self,oid,vnr):
        # ~ xml = self.T("meta","hwxml","hw_object.xml").format(
            # ~ SessionID = self.sessionid,
            # ~ ObjectID = oid,
            # ~ VersionNumber = vnr
        # ~ )        
        # ~ res = self.genRequest(xml)
        # ~ root = ET.fromstring( res["xml"][0] )
        # ~ tres = ET.ElementTree( root )
        
        
        # ~ breakpoint()
        
        # ~ print(res.raw.read())
        # ~ print(res.text)
        
        # ~ #print(res)
        
class HWReports(core.Core):
    
    def __init__(self,**kargs):
        super().__init__(**kargs)        
        
        self.N = kargs.get("N",-1)
        
        self.hw = HWClient(cen=self.cen)        
        self.hw.getSessionID(True)
        self.data = []
        
        self.output_files = kargs.get("output_files",False)
        
    
    def rptFileVersionsData(self,hwID,n=0,data=None):        
        
        
        #---------------------------------------------------------------
        #SETUP
        #---------------------------------------------------------------
        self.hw.getSessionID()        
        if type(hwID)==str:            
            res = self.hw.getObject(hwID)[0]
        else:            
            res = hwID
            hwID = res["HW_OID"]
        
        self.log.debug((" "*(n*4))+f"PROCESSING {hwID}")        
        
        data={
            "files":[],            
            "folders":[],
            "self":res
        }
        #---------------------------------------------------------------
        #PreProcess
        #---------------------------------------------------------------
        if res.get('DocumentType') == 'collection':
            for d in self.hw.getChildren(hwID):
                if d.get('DocumentType').lower() == 'collection':
                    data["folders"].append( self.rptFileVersionsData( d,n=n+1 ) )
                elif d.get('DocumentType').lower() in ['generic','text','report',]:
                    data["files"].append(d)
                else:
                    self.log.error(f"unknow type {d['HW_OID']}")                    
                    data["files"].append(d)
                
        return data
        
    def rptFileVersionsFromData(self,data,name=None,n=0,path=None,reset=False):
        self.log.debug("rptFileVersionsFromData start")
        if name == None:
            name = data["self"]["HW_OID"].replace(".","_")
        
        close_csv = False        
        if n==0:
            dt = datetime.datetime.now().strftime("%Y%m%d%H%M")        
            if reset==True:
                self.csvdoc = open(f"output/HW/hw_report_{name}.csv","wt",newline="\n")                
                self.w = csv.writer(self.csvdoc)
                self.w.writerow(["HW_ObjectName","Title","DocNumber","HW_OID","HW_Version","status","path"])
                self.mkReportFileDir(f"output/HW/hw_report_{name}_files",True)    
                
            else:
                self.csvdoc = open(f"output/HW/hw_report_{name}.csv","at",newline="\n")
                self.w = csv.writer(self.csvdoc)
                self.mkReportFileDir("output/HW/hw_report_{name}_files",False)    
            
            path = data["self"].get("HW_ObjectName","root")
            
        
        for d in data["files"]:  
            self.log.info("  "*n+f"file: {name}")
            if d.get(name,False) == False:
                self.log.info(f"adding {d['HW_OID']}")
                self.recordFile(d,path)
                d[name]=True
                
                
        
        for f in data["folders"]:
            self.rptFileVersionsFromData(f,name=name,n=n+1,path=path)
            
        
    #main function
    def rptFileVersions(self,hwID,n=0,path=""):        
                
        self.hw.getSessionID()
        res = self.hw.getObject(hwID)        
        self.log.info(f"Processing {hwID}")
        
        
        #is item is a folder?
        
        try:
            if res[0].get('DocumentType') == 'collection':
                #process folder
                path+=res[0].get("HW_ObjectName","noObjNm")
                
                self.log.info(f"Processing children of {hwID}")
                for item in self.hw.getChildren(hwID):
                    self.rptFileVersions( item["HW_OID"],n+1,path)
                
                
            elif res[0].get('DocumentType') == 'Generic':
                self.log.info(f"Processing {hwID} as a file")
                self.recordFile(res[0],path)
                        
                
            else:
                self.log.error(f"Processing {hwID} as a unknonw")
                self.log.error(f" "*4+f"{res[0].get('DocumentType','unknowntype')}")
                pprint(res[0])
                print("-"*80)
        
            
        except Exception as err:
            try:
                self.JD(res,"output","examples","hw_getobject.json")
                self.log.error(f'Failed to process {res[0].get("HW_OID","noId")}')
                self.log.error(err.args)
            except:
                self.log.error("FAILED to export response")
                breakpoint()
         
    def recordFile(self,data,path):        
        
        pre,oid_base,N = data["HW_OID"].split("-")
        N = int(N)
        oid = f"{pre}-{oid_base}-"
        
        for n in range(N):
            self.log.debug(f"Getting ver: {oid}{n+1}")            
            res = self.hw.getObject( f"{oid}{n+1}" )            
            try:
                self.writeReportData(res[0],path)
                if self.output_files == True:
                    self.writeFileData(res,path)
            except ValueError:
                continue
                
            
            
        if N > 1:
            self.csvdoc.flush()
        
    def writeReportData(self,data,path):
        if not data:
            raise ValueError
        
        
        title = data.get("Title","en:noTitle")
        m = re.match("(?:en:)?(.+)",title)
        if m:
            title = m.group(1)
        
        row = [
            data.get("HW_ObjectName","noObjNm"),
            title,
            data.get("DocNumber","noDocNr"),
            data.get("HW_OID","noId"),
            data.get("HW_Version","noVer"),
            data.get("Status","noStatus"),                
            path
        ]
        
        try:
            self.log.debug(row)
            self.w.writerow(row)
        except UnicodeEncodeError:
            try:
                new_row = [c.encode(errors ='ignore').decode() for c in row ]
            except Exception as err:
                print(err)
                self.log.error(row)
        
     
    def writeFileData(self,res,path):
        try:
            
            title = res[0].get("Title","en:noTitle")
            m = re.match("(?:en:)?(.+)",title)
            if m:
                title = m.group(1)
            
            title,ext = os.path.splitext(title)                        
            ext = mimetypes.guess_extension( res[0]["MimeType"],strict=False ) or  ext
            
            title = (title[:100]+ext).replace("/"," ")
            
            oid = res[0]["HW_OID"].replace(".","_").replace("-","_")
            
            file_path = os.path.join(self.report_files_base,path,f"{oid}_{title}")
            
            
            try:
                self.BW(res[1],file_path)
            except FileNotFoundError:
                self.log.debug(f"path is:{path} \nfilepath: {file_path} \nbase:{self.report_files_base}\ntitle:{title}\n\n")                
                os.makedirs( os.path.join( self.report_files_base, path ) )
                
                
                
                
            
            self.BW(res[1],file_path)
            
        except KeyError:
            breakpoint()
        
        
    def mkReportFileDir(self,path,clean_output = True):
        try:
            os.makedirs(path)
        except FileExistsError:
            if clean_output:
                self.delFolderTree(path)
                
        
        self.report_files_base = path
        
        
    def delFolderTree(self,path,n=0):
        for f in os.scandir(path):
            if f.is_file():
                os.remove( f.path )
            elif f.is_dir():
                self.delFolderTree(os.path.join(path,f.name),n+1)



#hyperwave
#http://172.29.42.179:8082/axis2/services/EskomHWService/





