import sqlite3

import re

 

 

 

def builddb() :

 

    global conn

    global db

    db = sqlite3.connect('c:\\temp\\ofx.db')

    db.execute("drop table request_ofxlog");
    db.execute("drop table response_ofxlog");

    db.execute("create table request_ofxlog (log_date DATETIME, Action varchar(20), module varchar(20), Session_Key varchar(35))")

    db.execute("create index ind1_ofxlog on request_ofxlog(Action , Session_Key)")

    db.execute("create index ind2_ofxlog on request_ofxlog( Session_Key)")   
   
   
    
    db.execute("create table response_ofxlog (log_date DATETIME, Action varchar(20), module varchar(20), Session_Key varchar(35))")
   
    db.execute("create index ind1_ofxlog2 on response_ofxlog (Action , Session_Key)")

    db.execute("create index ind2_ofxlog2 on response_ofxlog ( Session_Key)")   

    print ("Successfull building table & Index")

 

 

def loaddb() :

    print ("Program Start ")

    v_ct = 0

 

    rh = open("f:\\web_service_log_prod_audit_20131014.csv")

    # rh = open("c:\\tmp\\web_service_log_10142013.csv")

 

    for line in rh :


        
        line_arry = line.split(',', 5)

 

        match = re.search('customerInquiry|findBankingAccess|template', line)

        if match:

            if  (len (line_arry)) > 4 :

                v_ct = v_ct + 1

                
                if line_arry[2].strip() == "Request" :
                    db.execute ("insert into request_ofxlog values ('" + line_arry[0].strip() + "','" + line_arry[2].strip() + "','" + line_arry[1].strip() + "','" + line_arry[4].strip() + "')")
                else :
                     db.execute ("insert into response_ofxlog values ('" + line_arry[0].strip() + "','" + line_arry[2].strip() + "','" + line_arry[1].strip() + "','" + line_arry[4].strip() + "')")
 

    print (v_ct)

 

def reportResults():

 

    cursor = db.cursor()

 

    cursor.execute("SELECT count(*) from request_ofxlog")

    data = cursor.fetchone()

    print( "Records on Request table %d " % ( data))

    cursor.execute("SELECT count(*) from response_ofxlog")

    data = cursor.fetchone()

    print("Records on Response table %d " % (data)) 
    print ()

    print (" ----------- AVG TIME ------------------")

    sql1 = 'select AVG( ABS(strftime("%f", b.log_date ) - strftime("%f", a.log_date ))) , a.module from request_ofxlog  a, response_ofxlog b where a.session_key = b.session_key and a.action="Request" and b.action="Response" and a.module = b.module GROUP BY A.MODULE'           

    #sql3 = 'select distinct module from response_ofxlog'

    cursor.execute(sql1)

    resultset1 = cursor.fetchall()

    for row in resultset1:

         #print(row[0] + " " + row[1] + " " + row[2] + " " + row[3] + " " + row[4] + " " + row[5])
         print (" %f  %s "  %  (row[0]  , row[1] ))
         
    print()     
    print (" ----------- MAX TIME ------------------")
    sql2 = 'select MAX( ABS(strftime("%f", b.log_date ) - strftime("%f", a.log_date ))) , a.module from request_ofxlog  a, response_ofxlog b where a.session_key = b.session_key and a.action="Request" and b.action="Response" and a.module = b.module GROUP BY A.MODULE' 
    cursor.execute(sql2)

    resultset1 = cursor.fetchall()

    for row in resultset1:

         #print(row[0] + " " + row[1] + " " + row[2] + " " + row[3] + " " + row[4] + " " + row[5])
         print (" %f  %s "  %  (row[0]  , row[1] ))
  
 
    print()     
    print (" ----------- MIN TIME ------------------")
    sql2 = 'select MIN( ABS(strftime("%f", b.log_date ) - strftime("%f", a.log_date ))) , a.module from request_ofxlog  a, response_ofxlog b where a.session_key = b.session_key and a.action="Request" and b.action="Response" and a.module = b.module GROUP BY A.MODULE' 
    cursor.execute(sql2)

    resultset1 = cursor.fetchall()

    for row in resultset1:

         #print(row[0] + " " + row[1] + " " + row[2] + " " + row[3] + " " + row[4] + " " + row[5])
         print (" %f  %s "  %  (row[0]  , row[1] ))
  

def main ():

 

    builddb()

    loaddb()

    reportResults()

 

if __name__ == "__main__" :  main()
