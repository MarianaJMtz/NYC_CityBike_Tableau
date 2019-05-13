
# ETL Code
# This code will extract, transform and upload 2018 data from New York CityBike's System. 
# The resulting CSV file will be used to build a story dashboard in Tableau Desktop.


#Import dependencies
import os
import pandas as pd
from math import radians, sin, cos, acos


#Uploading Trip Files from CSV files

January_data = pd.read_csv("Trips_data/January.csv")
February_data = pd.read_csv("Trips_data/February.csv")
March_data = pd.read_csv("Trips_data/March.csv")
April_data = pd.read_csv("Trips_data/April.csv")
May_data = pd.read_csv("Trips_data/May.csv")
June_data = pd.read_csv("Trips_data/June.csv")
July_data = pd.read_csv("Trips_data/July.csv")
August_data = pd.read_csv("Trips_data/August.csv")
September_data = pd.read_csv("Trips_data/September.csv")
October_data = pd.read_csv("Trips_data/October.csv")
November_data = pd.read_csv("Trips_data/November.csv")
December_data = pd.read_csv("Trips_data/December.csv")


def distance_on_unit_sphere(row):
    try:
        slat = radians(row['start station latitude'])
        slon = radians(row['start station longitude'])
        elat = radians(row['end station latitude'])
        elon = radians(row['end station longitude'])
        
        #The earth radius is given directly in miles
        dist = 3958.8 * acos(sin(slat)*sin(elat) + cos(slat)*cos(elat)*cos(slon - elon))
        
    except:
        dist=0

    return dist


def cleaning_function(month_data):
    month_data=month_data.drop(columns=['stoptime'])
    month_data[['age']] = 2018 - month_data[['birth year']]
    month_data= month_data.drop(columns=['birth year'])
    month_data['starttime'] = month_data['starttime'].map(lambda x: str(x)[:-5])
    #Deleting age outliers, minimum age to get the card is 16 and we considered maximum as 99
    month_data = month_data[(month_data.age >= 16) & (month_data.age <=99)]
    #Assign more understandable values in gender
    replace_values = {0 : 'Unknown', 1 : 'Male', 2 : 'Female'}  
    month_data= month_data.replace({"gender": replace_values})
    #Get time in minutes
    month_data['tripduration']= round(month_data['tripduration']/60,2)
    #Rearranged Values
    month_data = month_data[['starttime', 'tripduration', 'start station id', 'start station name',
                            'start station latitude', 'start station longitude', 'end station id', 'end station name',
                            'end station latitude', 'end station longitude', 'bikeid', 'usertype', 'gender','age']]
    month_data['distances']= month_data.apply(distance_on_unit_sphere, axis=1)
    
    return(month_data)


January_data= cleaning_function(January_data)
February_data= cleaning_function(February_data)
March_data= cleaning_function(March_data)
April_data= cleaning_function(April_data)
May_data= cleaning_function(May_data)
June_data= cleaning_function(June_data)
July_data= cleaning_function(July_data)
August_data= cleaning_function(August_data)
September_data= cleaning_function(September_data)
October_data = cleaning_function(October_data)
November_data= cleaning_function(November_data)
December_data= cleaning_function(December_data)



Data_year2018 = pd.concat([January_data, February_data, March_data, April_data, May_data, June_data, July_data,
                          August_data, September_data, October_data, November_data, December_data], ignore_index=True)



Data_year2018.to_csv("Trips_Data_2018.csv", index=False)





