presc
=====
This Python code inserts data from a csv file into a Postgres database. 
It requires a file called postcodes.csv containing a list of Postcodes in the UK together with its latitude and longitude.
These postcodes have been downloaded from http://www.janyii.com/post_code/postcode.dbv3

The settings have been defined in presc.ini
To run press python presc.py, you will need the modules psycopg2, csv and cfgparse
The first time you can choose the option "create" to automatically create the database
Then you will need 2 csv files: 
go to http://www.hscic.gov.uk/searchcatalogue?q=title%3A%22presentation+level+data%22&area=&size=10&sort=Relevance
for prescriptions download TYYYYMMPDPI+BNFT.csv (where YYYY is the year and MM the month)
- link: "GP Practice Prescribing Presentation-level Data        Practice prescriptions data"
for practices download addresses TYYYYMMADDR+BNFT.csv (where YYY is the year and MM the month)
- link: "GP Practice Prescribing Presentation-level Data        Practice codes, names and addresses"  

To add these files to the database: run presc.py 
choose the option addpractice with the name of the csv file (TYYYYMMADDR+BNFT.csv) 
same with the option addpresc followed by the name of the csv file (TYYYYMMPDPI.csv)

