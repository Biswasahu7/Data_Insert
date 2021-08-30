# **************************
# CVML2 Data insert model.
# **************************

# Import necessary libraries...
import datetime
import time
import logging
from logging.handlers import TimedRotatingFileHandler
import pyodbc
import sys


# LOGGER
l1=datetime.datetime.now()
logger = logging.getLogger('OCR Logger')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler("/home/root1/CVML_2/Model_Logs/SQL_Logs/logs_{}.log".format(l1))
fh.setLevel(logging.INFO)
logger.addHandler(fh)


# logger = logging.getLogger("Rotating Log")
# logger.setLevel(logging.INFO)
# # add a time rotating handler
# handler = TimedRotatingFileHandler("/home/root1/CVML_2/Model_Logs/SQL_Logs/logs_{}.log".format(l1), when="m", interval=60)
# logger.addHandler(handler)

# ProcessMaster table all Process names...
"""('EBTBRICKJAM',),('EBTMASSFilling',),('EBTLevelling',),('HMPositioning',),('HMPouring',),('LaunderInsertion',),('TopLancePositioning',),('ScrapCharging',),('Gunning',),('Fettling',),
('TopRoofRemoval',),('SlagDoorCleaning',),('TopRoofPutBack',),('LaunderPouringBlowing',),('LaunderPouringArcing',),('TopPouringBlowing',),('TopPouringArcing',)  """

# Print all column name from ProcessMaster...
# cursor.execute("select * from ProcessMaster")
# result = cursor.fetchall()
# num_fields = len(cursor.description)
# field_names = [i[0] for i in cursor.description]
# print(field_names)

# Value required for insert to SQL DB Example ...
"""
# Column names required to insert Data
    # (Procseq, IsEnable, IsActive, liveProcessId, StartTime, EndTime, MstProcessId, Duration, SopTime, ShellNo,heatno)

    # Example Values to insert data
    #(1,'true','true',0,'2020-01-31 09:51:55.000','2020-01-31 09:56:04.000',4,0,100,1,200100444)

    1.Procseq - 1 (Procseq3)
    2.IsEnable - 'true'(IsEnable5)
    3.IsActive - 'true'(IsActive6)
    4.liveProcessId - 0 (ProcessId4)
    5.StartTime - '2020-01-31 09:51:55.000'
    6.EndTime - '2020-01-31 09:56:04.000'
    7.MstProcessId - 4 (MstProcessId0)
    8.Duration - EndTime - StartTime
    9.SopTime - 100 (SOP11)
    10.ShellNo - 1
    11.heatno - 200100444
"""

# *******************************************************************
# This function will help to insert data for CVMLAutomation2...

# From AI model we need pass one dictionary to run this function...

def insert_data(my_dict,result_file,shell_no):
    # Taking key from dict where model will pass... (Process name)
    # key = list(my_dict.keys())
    # processname = key[0]
    # # print("Process name-{}".format(processname))
    #
    # # Taking value from dict where model will pass...
    # values = list(my_dict.values())
    # stime = values[0][0]
    # etime = values[0][1]
    # shellN = values[0][2]

    # Connect CVML2 DB...
    print("Try to connect SQL Database for cvmlautomation2")

    # Give sql credentials...
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'Server=172.21.25.164;'
                          'Database=cvmlautomation2;'
                          'UID=sa;'
                          'PWD=admin@123;'
                          'MARS_connection=yes')

    cursor = conn.cursor()
    print("Connection successful for Database cvmlautomation2")
    logger.info("Connection successful for Database cvmlautomation2")

    # Covert model pass dictionary to list for running for loop...
    res = []
    for key, val in my_dict.items():
        res.append([key] + list(val))

    # Taking shell no from the dictionary...
    k = list(my_dict.values())
    # print(k[0][2])
    logger.info("Shell no - {}".format(k[0][2]))

    try:

        # Define a data class to store values...
        class data:

            # Define objects...
            def __init__(self, Procseq, IsEnable, IsActive, liveProcessId, StartTime, EndTime, MstProcessId, Duration,SopTime, ShellNo, heatno):

                # Assign value to object...
                self.Procseq = Procseq
                self.IsEnable = IsEnable
                self.IsActive = IsActive
                self.liveProcessId = liveProcessId
                self.StartTime = StartTime
                self.EndTime = EndTime
                self.MstProcessId = MstProcessId
                self.Duration = Duration
                self.SopTime = SopTime
                self.ShellNo = ShellNo
                self.heatno = heatno

        # Checking SQL connection...
        if cursor.connection:

            # Checking SQL connection for CVMLAutomation2...
            if cursor.connection:

                # Max value query from tbLL2HeatInfo...
                # heatno_query = "SELECT max (HEAT_NAME) FROM [HeatInfoDb].[dbo].[tblL2HeatInfo]"
                heatno_query = "SELECT Value FROM CVMLAutomation2.dbo.GeneralConfiguation where Name='Shell {} Heat Number' ".format(k[0][2])

                # Execute query...
                cursor.execute(heatno_query)

                # Getting max value from the table for heatno data...
                heatno1 = cursor.fetchall()
                heatno1 = heatno1[0][0]

                # if shell_no==4:
                #     new_heatno = int(heatno1) + 1
                # elif shell_no==3:
                new_heatno = int(heatno1)

                # print("heatno-{}".format(new_heatno))
                logger.info("Heat number has been capture -{}".format(new_heatno))
                with open("/home/root1/CVML_2/Result_Txt_shell{}/{}.txt".format(shell_no,result_file), "a") as f:
                    f.write("\n")
                    f.write("Heat Number-{}".format(new_heatno))

            for key in res:
                logger.info("model Dict Value-{},{},{},{}".format(key[0],key[1],key[2],key[3]))
                # print(key)

                # Taking process name from the list
                processname = key[0]

                # This query will get data from ProcessMaster table...
                query = "SELECT * FROM CVMLAutomation2.dbo.ProcessMaster WHERE ProcessAlias = '{}'".format(processname)
                print(query)

                logger.info("Select Query-{}".format(query))
                # Assign query...
                process_select_query = query

                # EXECUTE QUERY...
                cursor.execute(process_select_query)

                # Fetch all data and save into myresult variable...
                myresult = cursor.fetchall()

                # Create a empty list for append data...
                datalist = []
                for a in myresult:

                    # Append value to datalist...
                    datalist.append(a)

                # Taking Duration from end time to start time
                duration=int(abs(key[2] - key[1]).total_seconds())
                print("DURATION-{}".format(duration))
                # print(str(key[1]),type(str(key[2])))

                # Create a variable and assign value to class object...
                sqldata = data(datalist[0][3], datalist[0][5], datalist[0][6], datalist[0][4], key[1], key[2],datalist[0][0],duration, datalist[0][11], key[3], new_heatno)

                # print(sqldata.Procseq,sqldata.IsEnable,sqldata.IsActive,sqldata.liveProcessId,sqldata.StartTime,
                #       sqldata.EndTime,sqldata.MstProcessId,sqldata.Duration,sqldata.SopTime,sqldata.ShellNo,sqldata.heatno)

                # Data insert query...
                Data_insert_Query = "INSERT INTO liveSubProcess(Procseq, IsEnable,IsActive,liveProcessId,StartTime,EndTime,MstProcessId,Duration,SopTime,ShellNo,heatno) " \
                                    "VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');". \
                    format(sqldata.Procseq, sqldata.IsEnable, sqldata.IsActive, sqldata.liveProcessId,sqldata.StartTime, sqldata.EndTime, sqldata.MstProcessId, sqldata.Duration,sqldata.SopTime, sqldata.ShellNo, sqldata.heatno)

                print(" Data Insert query {}".format(Data_insert_Query))
                logger.info("Data Insert Query-{}".format(Data_insert_Query))

                # Insert query Code is execute...
                cursor.execute(Data_insert_Query)
                conn.commit()

                print("Data has been inserted")
                logger.info("Data has been insert")

                # # After execute code server will close...
                # cursor.close()
                # conn.close()
                # print("Data has been inserted")

        else:

            cursor.close()
            conn.close()

    except Exception as f:
        print("SQL connect failed for insert", f)
        logger.info("SQL connection failed for insert data",f)
        # exception_type, exception_object, exception_traceback = sys.exc_info()
        # filename = exception_traceback.tb_frame.f_code.co_filename
        # line_number = exception_traceback.tb_lineno
        # print("SQL Data insert Exception type: ", exception_type)
        # print("File name: ", filename)
        # print("Line number: ", line_number)

# t1=datetime.datetime.now().replace(microsecond=0)
# time.sleep(5)
# t2=datetime.datetime.now().replace(microsecond=0)
# # print(type(t1),t2)
# # print(t2-t1)
# duration=int(abs(t2-t1).total_seconds())
#
# my_dict = {"EBTLevelling": (t1,t2,1),
#            "EBTMASSFilling": (t1,t2, 2),
#            "HMPositioning": (t1,t2, 3)}
#
# insert_data(my_dict)