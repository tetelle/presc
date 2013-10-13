#!/usr/bin/python
# -*- coding: utf-8 -*-

""" MODULES REQUIRED """
import cfgparse
import cmd
import sys
import os
import csv   
import psycopg2	

""" GET SETTINGS IN PRESC.INI """
c=cfgparse.ConfigParser()
c.add_option('database',type='string')
c.add_option('path',type='string')
c.add_option('pc',type='string')
c.add_file('presc.ini')
opts=c.parse() # each time a database connection is required this command is used database='%s' %opts.database (a password could be added)

""" 
    ALL TABLES IN DATABASE 
    - pclatlng from a postcode gives latitude and longitude, values obtained from a large file postcodes.csv
    - Prescriptions: prescription details for practices, values obtained from TYYYYMMPDPI+BNFT.csv (year is YYYY MM is month)
    - Addressbook: address details for practices, values obtained from TYYYYMMPDPI+ADDR.csv (year is YYYY MM is month)
"""

TABLE_CREATE = """
 create table if not exists pclatlng (postcode TEXT PRIMARY KEY, latitude REAL, longitude REAL);

 create table if not exists Prescriptions 
 (pid serial PRIMARY KEY, 
  sha varchar(3),
  pct varchar(3),
  practice varchar(6) not null,
  bnf_code varchar(20),
  bnf_name varchar(50),
  items INT,
  nic real,
  act_cost real,
  quantity INT,
  period varchar(6),
  extra varchar(20),
  practice_id INT
);

create table if not exists Addressbook 
 (id serial, 
  entry_date varchar(6) default to_char(CURRENT_DATE, 'yyyymm'), 
  practice_id varchar(6) not null, 
  title varchar(50) not null,
  address_line1 varchar(50) not null,
  address_line2 varchar(50),
  city varchar(30), county varchar(30),
  area varchar(30),
  postcode varchar(8) not null,
  lat real,
  lon real,
  CONSTRAINT combo_practice PRIMARY KEY (entry_date,practice_id));
"""

"""
  ALL PROCEDURES IN DATABASE
  - get_practice_id(a,b) returns id and name of practice from a) a ref + b) a date 
  - get_practice_details(a,b) same as previous one but returns name and full address instead
  - get_coordinates(a) returns longitude and latitude from a string postcode
  - get_most_recent_practice(a) returns the most recent date and name of this practice from a practice id
"""


PROCEDURE_CREATE ="""
  CREATE OR REPLACE FUNCTION get_practice_id
  (IN p_ref VARCHAR, IN p_date VARCHAR,
  OUT p_id INT,OUT p_title VARCHAR) 
  RETURNS SETOF RECORD VOLATILE AS $$                                  
  BEGIN 
	RETURN QUERY 
	SELECT id,title
	FROM Addressbook
	WHERE practice_id=p_ref and entry_date=p_date; 
  END;
  $$LANGUAGE PLPGSQL;

  CREATE OR REPLACE FUNCTION get_practice_details
  (IN p_id VARCHAR,IN p_date VARCHAR,
  OUT p_title VARCHAR,OUT p_line1 VARCHAR,OUT p_line2 VARCHAR,OUT p_city VARCHAR,OUT p_area VARCHAR,OUT p_postcode VARCHAR) 
  RETURNS SETOF RECORD VOLATILE AS $$                                  
  BEGIN 
	RETURN QUERY 
	SELECT title,address_line1,address_line2,city,area,postcode 
	FROM Addressbook 
	WHERE practice_id=p_id and entry_date=p_date; 
  END;
  $$LANGUAGE PLPGSQL;

  CREATE OR REPLACE FUNCTION get_coordinates
  (IN pc VARCHAR, OUT lat REAL, OUT lon REAL) 
  RETURNS SETOF RECORD VOLATILE AS $$                
  BEGIN 
  	RETURN QUERY 
  	SELECT latitude,longitude 
  	FROM pclatlng 
  	WHERE postcode=pc; 
  END; 
  $$LANGUAGE PLPGSQL;

  CREATE OR REPLACE FUNCTION get_most_recent_practice(IN p_id VARCHAR,OUT max TEXT,OUT p_title VARCHAR) 
  RETURNS SETOF RECORD VOLATILE AS $$                                                                                                       
  BEGIN                                                                                                                                     
  RETURN QUERY                                                                                                                              
  SELECT max(entry_date),title                                                                                                              
  FROM Addressbook                                                                                                                         
  WHERE practice_id=p_id group by title;                                                                                                    
  END;
  $$LANGUAGE PLPGSQL;"""

"""
  ALL FUNCTIONS IN DATABASE
  Only my_trigger_function() which returns the id in database of a practice from its reference and a date 
  returns 0 if the period (a valid date) has not been provided
"""

FUNCTION_CREATE ="""
  CREATE OR REPLACE FUNCTION my_trigger_function()
  RETURNS trigger AS '
  BEGIN
    IF NEW.period IS NULL  THEN
      NEW.practice_id := 0;
    ELSE
	    NEW.practice_id := (select id from Addressbook WHERE practice_id=NEW.practice and entry_date=NEW.period);
    END IF;
    RETURN NEW;
  END' LANGUAGE 'plpgsql';"""

"""
  ALL TRIGGERS IN DATABASE
  my_trigger is the only trigger on table Prescriptions calling the above function when inserting a new row
"""

TRIGGER_CREATE ="""
  DROP TRIGGER IF EXISTS my_trigger ON Prescriptions;
  CREATE TRIGGER my_trigger 
  BEFORE INSERT ON Prescriptions
  FOR EACH ROW
    EXECUTE PROCEDURE my_trigger_function();"""

"""
  ALL INDEXES IN DATABASE
  Creating indexes improve the speed of queries
  idx_presc_prac     creates an index on the field practice in the table Prescriptions
  idx_presc_bnfcode  create an index on the field bnf_code in the table Prescriptions
"""

INDEX_CREATE  ="""
  DROP INDEX if exists idx_presc_prac;
  create index idx_presc_prac on Prescriptions(practice);
  DROP INDEX if exists idx_presc_bnfcode;
  create index idx_presc_bnfcode on Prescriptions(bnf_code); """


"""
--------------------------------------------------------------------------------------
                 ALL FUNCTIONS CALLED BY THE PARSER
--------------------------------------------------------------------------------------
"""

""" MAKE DATABASE FROM SETTINGS IN PRESC.INI """
def create_database(): 
  try:
    con = psycopg2.connect(database='%s' %opts.database)
    cur = con.cursor()
    
    # Create tables
    cur.execute(TABLE_CREATE)
    print "\tcreating tables\t\t...30%"

    # Get postcodes and longitude/latitude coordinates
    cur.execute("select count(postcode) from pclatlng")
    total = cur.fetchone()
    if total is not None and total[0]>0:
      print "\tpostcodes checked\t...40%"
    else:
      query="COPY pclatlng FROM %s USING DELIMITERS ',' CSV" %opts.pc
      print "\t",query
      cur.execute(query)
      print "\tinserting postcodes\t...40%"

    # Create procedures
    cur.execute(PROCEDURE_CREATE)
    print "\tcreating procedures\t...80%"

    # Create indexes
    cur.execute(INDEX_CREATE)
    print "\tcreating indexes\t...100%"

    # Submit and save all these changes
    con.commit()
    print "Done"
  except psycopg2.DatabaseError, e:
		print "Database Error"

""" CHECK IF THESE PRESCRIPTIONS ARE ALREADY IN DATABASE
    No need to add the csv file prescriptions if (practice,date) are already in the table Prescriptions        
    This function returns True if there is no need to do the insert, False if the insert is required """
def check_prescription(filename):
  csv.field_size_limit(320000)
  try:
    with open(filename,"r") as input:   
      reader=csv.reader(input)
      line1=reader.next()
      line2=reader.next() # get values on the second line of the csv file (not the header)
      con = psycopg2.connect(database='%s' %opts.database)
      cur = con.cursor()
      query="select * from Prescriptions where practice='%s' and period='%s' " %(line2[2],line2[9]) # check these values are in the database
      cur.execute(query)
      result=cur.fetchone()
      if result is not None: return True
      else: return False
  except:
    print "An error has occured (does the table Prescriptions exist?)"
    return True

""" GET DATA PRESCRIPTIONS COPY TO DATABASE """
def insert_prescription(filename): # FROM A CSV FILE <...PDPI+BNFT.csv> 
  try:
    con = psycopg2.connect(database='%s' %opts.database)
    cur = con.cursor()

    if filename!="" and filename.find(".csv")>0: # make sure it is a csv file
      myfile="\'%s/%s\'" %(opts.path ,filename)
      query="COPY Prescriptions (sha,pct,practice,bnf_code,bnf_name,items,nic,act_cost,quantity,period,extra) FROM %s USING DELIMITERS ',' WITH NULL AS '' CSV HEADER" %myfile
      print query
      cur.execute(query)
      con.commit() 
      print "Done"  

    else: # it is not a csv file or no name has been provided
      print "Syntax: addpresc ___.csv"
  except psycopg2.DatabaseError, e:
    print "Database Error"

""" CHECK IF A DATE HAS ALREADY BEEN INSERTED IN THE DATABASE """
def check_date(myrow,myfilename): #  CSV file format <...ADDR+BNFT.csv> 
  try:
    # Connect to database, is there any data for this date?
    con = psycopg2.connect(database='%s' %opts.database) 
    cur = con.cursor() 
    cur.execute("select count(*) from Addressbook where entry_date='"+myrow[0]+"'") 
    result = cur.fetchone()

    if result is not None: # data for this date exist
      counter=0
      with open(myfilename,"r") as myinput: 
        for r in myinput:
          counter+=1 # check how many rows in file
      
      if counter==result[0]: # same number of rows in file than in database
        return True
      else: # different number of rows in file compared to the database
        insert_in_db(myrow) # insert in database
        return False

    else: #date not in file, new data
      insert_in_db(myrow) # insert in database
      return False

  except psycopg2.DatabaseError, e:
    print "Database Error"
    return True

""" DATABASE NEEDS UPDATING WITH NEW VALUES (MYROW) """
def insert_in_db(myrow): 
  ROW_INSERT = """insert into Addressbook (entry_date,practice_id,title,address_line1,address_line2,city,area,postcode,lat,lon) values """
  try:
    con = psycopg2.connect(database='%s' %opts.database) 
    cur = con.cursor()

    # get all individual values in this row, search for postcode to fetch its coordinates
    newvalues = [w.replace('\'', '') for w in myrow] #avoid problem of apostrophe   
    mypostcode=myrow[7].upper().replace(" ","") #convert postcode into uppercase and remove spaces
    cur.execute("select * from get_coordinates('"+mypostcode+"')") #get coordinates
    fetch_coords = cur.fetchone()

    # retrieve longitude and latitude, set it to 0 by default
    if fetch_coords is not None: #values for latitude and longitude
      newvalues.append(fetch_coords[0])
      newvalues.append(fetch_coords[1])
    else: #no latitude and longitude for this postcode
      newvalues.append(0)
      newvalues.append(0)
    
    # insert these values in the table Addressbook
    QUERY_INSERT=ROW_INSERT + str(tuple(newvalues)) # add this row with its latitude and longitude coordinates
    cur.execute(QUERY_INSERT)
    con.commit()  

  except psycopg2.DatabaseError, e:
    print "Database Error"

""" READ CSV FILE AND PUT DATA IN DATABASE """
def insert_practice(filename): # CSV file format <...ADDR+BNFT.csv> 
	try:
  		con = psycopg2.connect(database='%s' %opts.database) 
  		cur = con.cursor()
  
  		print "Reading csv and checking db..."
  		csv.field_size_limit(320000)
  		with open(filename,"r") as input: 
  			reader=csv.reader(input) # read csv 
  			if not check_date(reader.next(),filename): #read first line of data, if this date is not in already in database insert it
  				print "Inserting data..."
  				for row in reader: #continue reading csv line per line
  					insert_in_db(row) # insert this row in database and retrieve its coordinates
  				print "Done"
  			else: # this date has already been inserted in database, no need to carry on (data in csv file is for one date only, format TYYYYMM+BNFT.csv)
  				print "This time period has already been inserted into database"

	except psycopg2.DatabaseError, e:
		print 'Database error: %s' % e    

""" ------------------------------ """
"""     CONFIG PARSER - MAIN MENU  """
""" ------------------------------ """

class CLI(cmd.Cmd):
  # Init function
  def __init__(self):
    cmd.Cmd.__init__(self)
    self.prompt = '> '

  # Help: function create database, how to use it  
  def help_create(self):
    print "syntax: create",
    print "-- Creates all the database"

  # Help: function add practices to the database, how to use it 
  def help_addpractice(self):
		print "syntax: addpractice",
		print "-- Add the practices csv file into the database"

  # Help: function add prescriptions to the database, how to use it 
  def help_addpresc(self):
		print "syntax: addpresc",
		print "-- Add the prescriptions csv file into the database"

  # Help: function quit, how to use it 
  def help_quit(self):
		print "syntax: quit",
		print "-- Terminates the application"
	
	# Quit
  def do_quit(self, arg):
		sys.exit(1)

	# Creates the database, to use only the first time
  def do_create(self,arg):
		print "Creating database..."
		create_database()
	
  # Add a csv file containing practices
  def do_addpractice(self, arg):
    if arg!="" and arg.find(".csv")>0: #user provided the name of a csv file
      if os.path.exists(arg): # this file exists 
        insert_practice(arg)  
      else: 
        print "This file does not exist" # error message
      
    else: 
      print "Syntax: addpractice ___.csv"
	
	# Add a csv file containing prescriptions
  def do_addpresc(self, arg): 
    if arg!="" and arg.find(".csv")>0: #user provided the name of a csv file
      if os.path.exists(arg): # this file exits
        if check_prescription(arg): 
          print "Data has already been inserted"
        else:
          print "Inserting csv prescription..."
          insert_prescription(arg)
      else: 
        print "This file does not exist"    
    else:
      print "Syntax: addpresc ___.csv"

  # shortcut 
  do_q = do_quit


# MY CONFIG PARSER: EXECUTES COMMANDS ABOVE UNTIL USER QUITS
print "******************************************************************************"
print "* Practices and Prescriptions Database - type help for assistance, q to quit *"
print "******************************************************************************"
cli = CLI()
cli.cmdloop()
