import sqlite3
import re
import time
#from datetime import datetime as dt
#from xml.etree.ElementTree import ElementTree
from xml.etree import ElementTree as ET

from xml.dom.minidom import parse
import xml.dom.minidom
import datetime as dt



def builddb() :

 

    global conn
    global dbcursor
    global db
    global xmldic
    global dic
    global v_ct  
    
    dic = dict()
    xmldic = dict()
    db = sqlite3.connect('c:\\temp\\ofxrequest.db')

    db.execute("drop table ofxrequest");
    db.execute("drop table ofxtiming");

    db.execute("create table ofxrequest (request_id NUMBER(6), xaid NUMBER(10), subtask NUMBER(3), instance_nbr NUMBER(6), create_date DATETIME, modify_date DATETIME, request_type VARCHAR(20), recs_processed NUMBER(6))")

    db.execute("create table ofxtiming (request_id NUMBER(6), xaid NUMBER(10), subtask NUMBER(3), instance_nbr NUMBER(6), create_date DATETIME, modify_date DATETIME, request_type VARCHAR(20), recs_processed NUMBER(6), duration TIME)")


    db.execute("create index ind1_ofxrequest on ofxrequest(xaid , subtask)")

    db.execute("create index ind2_ofxrequest on ofxrequest( create_date)")   
   
 

    print ("Successfull building table & Index")

 

 

def loaddb() :

    print ("Program Start ")

    v_ct = 0

 

    #rh = open("f:\\ofxrequest.csv")

    rh = open("c:\\temp\\extract_request_audit_201316.csv")


    for line in rh :
        match = re.search('No Change', line)
        if match:
            v_ct = v_ct + 1
            line_arry = line.split(',', 13)

           
            ext_line = (line_arry[4].replace('encoding="UTF-8" standalone="yes"', " "))
            extractXML(ext_line.replace('xmlns:ns2="http://service.wellsfargo.com/entity/message/2007/"',' '))

   #db.execute("create table ofxrequest (request_id NUMBER(6), thread_id VARCHAR(30), subtask NUMBER(3), instance_nbr NUMBER(6), create_date DATETIME, modify_date DATETIME, request_type VARCHAR(20), recs_processed NUMBER(6))")   
            db.execute ("insert into ofxrequest values ('" + line_arry[0].strip() +             # Request_id [0]
                                                   "','" + line_arry[1].strip() +               # XAID
                                                   "','" + line_arry[7].strip() +               #subtask
                                                   "','" + line_arry[5].strip() +               #instance
                                                   "','" + line_arry[9].strip() +               #Create Date
                                                   "','" + line_arry[10].strip() +              #Modify_Date
                                                   "','" + line_arry[2].strip() +               #Request_type
                                                   "','" + xmldic["recsprocess"] +              #Recs Processed
                                                   "')"   )
               

    print (v_ct)
    
def extractXML( xmldata):

    
    xml_split = xmldata.split('>')
    recsprocess = (xml_split[7].split('<')[0])
    
    threadid = (xml_split[13].split('<')[0])  
    
    xmldic["threadid"] = threadid
    xmldic["recsprocess"] = recsprocess

def scandb () :
    dbcursor = db.cursor()
    sql1 = "select * from ofxrequest order by instance_nbr , create_date"
    

    duration = time.time()
    
    dbcursor.execute(sql1)
    resultset1 = dbcursor.fetchall()
 
    
    for row in resultset1:
        
            fs="%Y-%m-%d %H:%M:%S.%f"
            create_time=dt.datetime.strptime(row[4], fs)
            modify_time=dt.datetime.strptime(row[5], fs)

            duration = modify_time - create_time
            #print(modify_time.time(), create_time.time()  , duration )

            
            

            dic["requestID"] = row[0]
            dic["xaid"] = row[1]
            dic["subtask"] = row[2]
            dic["instance"] = row[3]
            dic["createDT"] = row[4]
            dic["modifyDT"] = row[5]
            dic["requesttype"] = row[6]
            dic["recsprocessed"] = row[7]
            dbInsert(dic, duration)
            dic.clear()
        
def dbInsert( dic, duration):
    
    
    dic["recsprocessed"] = 0
    
    sql1 = "insert into ofxtiming values (" + str(dic["requestID"]) + ",'" + str(dic["xaid"]) + "'," + str(dic["subtask"]) + ","  + str(dic["instance"]) + ",'" + str(dic["createDT"]) + "','" + str(dic["modifyDT"]) + "','" + dic["requesttype"] + "'," + str(dic["recsprocessed"]) + ",'" + str(duration) + "')"

    
    dbcursor = db.cursor()

    dbcursor.execute(sql1)
    
def reportResults():

    cursor = db.cursor()

    cursor.execute("SELECT count(*) from ofxtiming")

    data = cursor.fetchone()

    print( "Records on Timing table %d " % ( data))

    print (" ----------- Diff Duration-----------------")

    #sql1 = 'select AVG( ABS(strftime("%f", a.modify_date ) - strftime("%f", a.create_date ))) , a.instance_nbr from ofxtiming A  GROUP BY A.instance_nbr'           
    sql1 = 'select strftime("%f", a.modify_date) , strftime("%f", a.create_date) , a.instance_nbr from ofxtiming A  GROUP BY A.instance_nbr order by create_date'

    cursor.execute(sql1)

    resultset1 = cursor.fetchall()

    for row in resultset1:

         print (" %s  %s %s"  %  (row[0]  , row[1] , row[2]))
         
    print() 



    print (" ----------- AVG TIME ------------------")

    #sql1 = 'select AVG( ABS(strftime("%f", a.modify_date ) - strftime("%f", a.create_date ))) , a.instance_nbr from ofxtiming A  GROUP BY A.instance_nbr'           
    sql1 = 'select AVG( A.duration) ,A.instance_nbr from ofxtiming A  GROUP BY A.instance_nbr '
    

    cursor.execute(sql1)

    resultset1 = cursor.fetchall()

    for row in resultset1:

         print (" %s  %s "  %  (row[0]  , row[1] ))
         
    print() 
    
       
    #print (" ----------- MIN TIME ------------------")
    #sql2 = 'select xaid, MIN( A.duration),  a.instance_nbr, strftime("%f", a.create_date) , strftime("%f", a.modify_date)  from ofxtiming A group by xaid, instance_nbr create_date, modify_date order by instance_nbr'
    #print(sql2)
    #cursor.execute(sql2)

    #resultset1 = cursor.fetchall()

    #for row in resultset1:

         #print(row[0] + " " + row[1] + " " + row[2] + " " + row[3] + " " + row[4] + " " + row[5])
         #print (" %s  %s %s %s %s"  %  (row[0]  , row[1] , row[2] , row[3] , row[4]))
  
    
    print (" ----------- MAX TIME ------------------")
    #sql2 = 'select MAX( ABS(strftime("%Y-%m-%d %H:%M:%S.%f", a.modify_date ) - strftime("%Y-%m-%d %H:%M:%S.%f", a.create_date ))) , a.instance_nbr a.xaid from ofxtiming A  GROUP BY a.instance_nbr, A.xaid' 
    sql2="select  a.xaid, MAX( A.duration), a.create_date , a.modify_date, a.instance_nbr from ofxtiming  a  group by xaid, a.create_date, modify_date, a.instance_nbr order by a.create_date, a.request_id  "
    
    cursor.execute(sql2)
    
    resultset1 = cursor.fetchall()

    fo = open("c:\\temp\\xaid_performance.csv", "w")


    for row in resultset1:

         #print(row[0] + " " + row[1] + " " + row[2] + " " + row[3] + " " + row[4] + " " + row[5])
         fo.write( str(row[0]) + ";"  + str(row[1]) + ";"  + str(row[2]) + ";" + str(row[3]) + ";"  + str(row[4]) + "\n")

    
    print() 
    fo.close()
   


def main ():

    builddb()
    loaddb()
    scandb()
    reportResults()

 

if __name__ == "__main__" :  main()
