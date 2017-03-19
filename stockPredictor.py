# First project assignment - Software Engineering of Web Applications
# Dependencies: yahoo-finance, mysql.connector Compilation: Execution:
#
# Team: Jose Wilfredo Aleman Espinoza,  ja918
# Demetrios Lambropoulos                dpl60
# David Lambropoulos                    dal220
# Fahad Mohammad -                      fm244
# Bharath Joginapally                   bj196
#
#
from __future__ import print_function
from yahoo_finance import Share
import datetime
import mysql.connector
import time

# it is recommended to use a config file insted of hardwritting the login info in the code


def main():
    print ("Welcome to Data Collection")

    stockslist = ['YHOO','GOOG','TSLA','AAPL','NVDA'] # I removed SQ since doesn't have too much historical data, we need to handle error
                                                      # If we want to add it.... 

    print ("The program retrieves information for the following stocks: \n" + " ".join(stockslist))

    addmore = 'y'

    addmore = raw_input("Do you want to add a new stock? (y/n): ")

    while addmore == 'y':
        newstock = raw_input("To add a new stock type the tricker and press enter: ")
        try:
            newshare = Share(newstock)
            print (newshare.get_price())
            stockslist.append(newstock)
            break
        except ValueError:
            print ("The stock input is not valid, try again")
        addmore = raw_input("Do you want to add a new stock? (y/n): ")
          
    # We must first run the historical
    historical(stockslist) 
    # Then the RealTime
    #
    print("Now the program will retrive real time data...")
    RealTime(stockslist,3)    

    print("\nThanks for using this Software. \n"
          "\nCredits: \n"
          "* Jose Wilfredo Aleman Espinoza,   ja918\n"
            "* Demetrios Lambropoulos         dpl60\n"
            "* David Lambropoulos             dal220\n"
            "* Fahad Mohammad                 fm244\n"
            "* Bharath Joginapally            bj196")

def RealTime(stocks,count):

    cnx = mysql.connector.connect(user='root', password='123',
                              host='127.0.0.1',
                              database='DBtest')
    cursor = cnx.cursor()

    waitsec = 60
    waitmessage = "Wait 1 minute for the next real-time values..."
    
    #The following lines avoid running the program multiple times if the market it's closed
    now = datetime.datetime.utcnow()     # Check the current time              
    today14= now.replace(hour=14, minute=25, second=0, microsecond=0)
    today21= now.replace(hour=21, minute=5, second=0, microsecond=0)
       
    if (now >= today21 or today14 >= now):
        count = 1
        waitsec = 1
        waitmessage = ""
        print ("The market opens at 14:30 UTC and closses at 21:00 UTC, \n***Data will be retrieved only once.")
        print ("Current datetime " + str(now))
        
    while 0 < count:
        
        for i in stocks:
            print ("Stock of " + i)
            stock = Share(i)
            
            stock.refresh()
            
            price = stock.get_price()
            
            stime = stock.get_trade_datetime()
            # Needed to fix the format the date is retrieved from Yahoo Finance
            timefixed = stime[:-8]               

            volume = stock.get_volume()
            
            print ("Price: " + price + " Time: " + timefixed + " Volume: " + volume)

            # Don't forget to use the config file to avoid writing the password on the source

        
            add_realtime = ("INSERT INTO realtime "
                           "(tricker, price, time, volume) "
                           "VALUES (%s, %s, %s, %s)")
        
            data_realtime = (i, price,timefixed, volume)

            try:
                cursor.execute(add_realtime, data_realtime)
                cnx.commit() # Make sure data is committed to the database
            except mysql.connector.IntegrityError as err:
                #print(err)  # Ussually a Duplicate value error
                pass


        print ("Database sucessfuly udpated. " + waitmessage)          
        time.sleep(waitsec)
        count-=1

    cursor.close()
    cnx.close()

def historical(stocks):
    cnx = mysql.connector.connect(user='root', password='123',
                              host='127.0.0.1',
                              database='DBtest')
    cursor = cnx.cursor()

    databegining = raw_input("Please type the begin date for the historical data"
                         "in the format YYYY-MM-DD : ")

    datafinish = raw_input("Please type the last date for the historical data"
                        "in the format YYYY-MM-DD : ")
       
    for i in stocks:
        print ("Stock of " + i)
        stock = Share(i)

        # Provides the information on a Dictionary
        name = stock.get_name()
        history = stock.get_historical(databegining,datafinish)
        

        print ("Getting the data for: " + i + " " + name)
        print ("|  Date  |  Opening  |  High  | "
               " Low  |  Close  |  Volume  |")
        
        for j in history:
            date = j['Date']
            opening = j['Open']
            high = j['High']
            low = j['Low']
            close = j['Close']
            volume = j['Volume']

            newrecord = (date + " | " + opening + " | " + high + " "
                         "| " + low + " | " + close + " | " + volume)

            print (newrecord)  

        # Don't forget to use the config file to avoid writing the password on the source

            add_realtime = ("INSERT INTO historical "
                           "(tricker, date, open, high, low, close, volume) "
                           "VALUES (%s, %s, %s, %s, %s, %s, %s)")

            data_realtime = (i, date, opening, high, low, close, volume)

            try:
                cursor.execute(add_realtime, data_realtime)
                cnx.commit() # Make sure data is committed to the database
            except mysql.connector.IntegrityError as err:
                #print(err)  # Ussually a Duplicate value error
                pass
        

    print ("Database sucessfuly udpated. ")          
        

    cursor.close()
    cnx.close()


    

main()
