import sqlite3
import os
import json
import logging
import csv
import datetime

import bson
    
    
def jsonDefault(obj):
    
    if type(obj) == bson.objectid.ObjectId:
        return str(obj)
    elif type(obj) == datetime.datetime:
        return obj.strftime("%Y-%m-%dT%H:%M:%S")
    elif type(obj) == datetime.date:
        return obj.strftime("%Y-%m-%d")
    elif type(obj) == datetime.time:
        return obj.strftime("%H:%M:%S")    
    else:
        return str(obj)
    


class Central():
    
    def __init__(self):        
        # ~ self.mg_db = pymongo.MongoClient(port=27005)
        self.sql_db = sqlite3.connect("meta/main.db")
        
        
        with open("meta/meta.json","rt") as inj:            
            self.meta = json.load(inj)

        try:
          self.log =  logging.getLogger("CENTRAL")
        
          self.cen_log = logging.FileHandler("logs/CENTRAL.csv",mode="w")    
          self.cen_log.setFormatter( logging.Formatter( "\"{asctime}\",{name},{levelname},\"{msg}\"",style="{"))
        except Exception:
          print("Central cannot log")
        
        
        



class Core():
    
    def __init__(self,**kargs):
        
        self.cen = kargs.get("cen",Central())        
        self.name = kargs.get("name",self.__class__.__name__)
        self.log = self.LOG(self.name)
            
        self.log.info("ONLINE")
            
    
    def T(self,*fl):
        fl = os.path.join(*fl)
        
        with open(fl,"rt") as txt:
            return txt.read()

    def B(self,*fl):
        fl = os.path.join(*fl)  
        with open(fl,"rb") as txt:
            return txt.read()

    def BW(self,data,*fl):
        fl = os.path.join(*fl)  
        with open(fl,"wb") as txt:
            return txt.write(data)

    def TW(self,data,*fl):
        fl = os.path.join(*fl)  
        with open(fl,"wt") as txt:
            return txt.write(data)


    def J(self,*fl):
        fl = os.path.join(*fl)
        
        with open(fl,"rt") as txt:
            return json.load(txt)

    def JD(self,data,*fl,**kargs):
        fl = os.path.join(*fl)  

        os.makedirs( os.path.split(fl)[0], exist_ok=True )
        
        kargs["indent"] = kargs.get("indent",4)
        kargs["default"] = kargs.get("default",jsonDefault)
        
        with open(fl,"wt") as txt:
            json.dump(data,txt,**kargs)

    
    
    def CSV(self,head,row,fn):
        with open(fn,"wt",newline="\n") as cout:
            w = csv.writer(cout)        
            w.writerow(head)
            for row in data:
                w.writerow(row)
    

    def LOG(self,name):        
        log = logging.getLogger(name)
        
        sh = logging.StreamHandler()    
        sh.setFormatter( logging.Formatter( "{asctime} {name} {levelname}:{msg}",style="{"))
        log.addHandler(sh)
        
        sh = logging.FileHandler("logs/{}.csv".format(name),mode="a")    
        sh.setFormatter( logging.Formatter( "\"{asctime}\",{name},{levelname},\"{msg}\"",style="{"))
        log.addHandler(sh)        
        log.addHandler( self.cen.cen_log )        
        
        log.setLevel(logging.DEBUG)
        
        return log


    
