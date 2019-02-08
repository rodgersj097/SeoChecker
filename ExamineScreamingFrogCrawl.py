#This Script opens xampp and starts the mysql server for localhost. 
#Then extracts the new crawl data from Screaming frog that runs every monday morning 
#and cross references it to the previous weeks lists and find any new error for Client Errors 0xx, Redirection error (3xx) and Response Errors(4xx). 
#once it has found the new errors it adds it to the master list as well as creating a new Excel sheet listing the errors.
#It then deletes all duplicate rows as most of these errors wont be fixed in a week and gets the table ready for next week.
#Script Created by Jacob Rodgers 02/07/2019 
########################################################################DO NOT CHANGE ANYTHING AFTER THIS LINE #######################################################################################################
import csv
import MySQLdb
import time 
from pywinauto import Application
start = time.time() 
#Open Xampp and start Mysql Server ##################################################################################################################################################################################
app = Application().Start(cmd_line=u'"C:\\xampp\\xampp-control.exe" ')
xampp = app.TfMain
xampp.wait('ready')
mysqlStart = xampp.Start4 
mysqlStart.click()
#Open connection  ###################################################################################################################################################################################################
mydb = MySQLdb.connect(host='localhost',
user='root',
passwd='',
db='screamingfrog', use_unicode=True, charset="utf8")
cursor = mydb.cursor()

#Open the csv and store in var  ####################################################################################################################################################################################
csv_data = csv.reader(open('C:/Users/rodgersja/Documents/MondayCrawls/client_error_(4xx)_inlinks.csv'))
csv_data2 = csv.reader(open('C:/Users/rodgersja/Documents/MondayCrawls/redirection_(3xx)_inlinks.csv', encoding="utf8"))
csv_data3 = csv.reader(open('C:/Users/rodgersja/Documents/MondayCrawls/no_response_inlinks.csv', encoding="utf8"))
#insert the new scrape into the holder tables  #####################################################################################################################################################################
next(csv_data)
next(csv_data2)
next(csv_data3)
for row in csv_data:
    cursor.execute('INSERT INTO client_error(type, source, Destination, Size , AltText, Anchor, Statuscode,Status,Follow  ) VALUES("%s", "%s", "%s",%s, "%s", "%s",%s, "%s","%s")' , row)
for row in csv_data2:
    cursor.execute('INSERT INTO redirection(type, source, Destination, Size , AltText, Anchor, Statuscode,Status,Follow  ) VALUES("%s", "%s", "%s",%s, "%s", "%s",%s, "%s","%s")' , row)
for row in csv_data3:
    cursor.execute('INSERT INTO response(type, source, Destination, Size , AltText, Anchor, Statuscode,Status,Follow  ) VALUES("%s", "%s", "%s",%s, "%s", "%s",%s, "%s","%s")' , row)
print("CSV to table done")
#find the new errors and store them in result ########################################################################################################################################################################
mydb.commit()
cursor.close()
result = [] 
cursor = mydb.cursor()
cursor.execute("SELECT t1.* FROM client_error t1 WHERE ( t1.Source, t1.Destination, t1.Anchor) NOT IN (SELECT  t2.Source, t2.Destination, t2.Anchor from client_error_master t2);") 
result.append(cursor.fetchall())
print("Found Unique 1/3")
cursor.execute("SELECT t1.* FROM redirection t1 WHERE ( t1.Source, t1.Destination, t1.Anchor) NOT IN (SELECT  t2.Source, t2.Destination, t2.Anchor from redirection_master t2);") 
result.append(cursor.fetchall())
print("Found Unique 2/3")
cursor.execute("SELECT t1.* FROM response t1 WHERE ( t1.Source, t1.Destination, t1.Anchor) NOT IN (SELECT  t2.Source, t2.Destination, t2.Anchor from response_master t2) ; ") 
result.append(cursor.fetchall())
print("Found Unique 3/3")
#Move new errors into master so they dont show next week if it hasnt been fixed  #######################################################################################################################################
cursor.close()
cursor = mydb.cursor()
print("Starting to add unique entries to the master databases")
try: 
    cursor.execute("""
    insert into client_error_master
    SELECT t1.* FROM client_error t1 WHERE
    ( t1.Source, t1.Destination, t1.Anchor) 
    NOT IN (SELECT  t2.Source, t2.Destination, t2.Anchor from client_error_master t2); 
    """)
    print("add unique to master 1/3")
    cursor.execute("""
    insert into redirection_master
    SELECT t1.* FROM redirection t1 WHERE
    ( t1.Source, t1.Destination, t1.Anchor) 
    NOT IN (SELECT  t2.Source, t2.Destination, t2.Anchor from client_error_master t2); 
    """)
    print("add unique to master 2/3")
    cursor.execute("""
    insert into response_master
    SELECT t1.* FROM response t1 WHERE
    ( t1.Source, t1.Destination, t1.Anchor) 
    NOT IN (SELECT  t2.Source, t2.Destination, t2.Anchor from client_error_master t2); 
    """)
except MySQLdb._exceptions.IntegrityError as e: 
    print("Error", e)
    
print("add unique to master 2/3")
#delete temp tables  ##################################################################################################################################################################################
cursor.execute("delete from client_error")
cursor.execute("delete from redirection")
cursor.execute("delete from response")
print("deleted duplicates")
#write new errors to csv for better distrobution  #####################################################################################################################################################
c = csv.writer(open('C:/Users/rodgersja/Documents/MondayCrawls/newErrors.csv','w'))
for x in result:
    for y in x:
        c.writerow(y)
#Commite changes to Database and close connection to cursor and database. then calculate run time  ###################################################################################################
mydb.commit()
cursor.close()
app.kill_()
mydb.close() 
print("Done")
end = time.time() 
print(start - end)