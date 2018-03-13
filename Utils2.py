import pandas as pd
import blpapi
import datetime as dt
from sqlalchemy import create_engine
from optparse import OptionParser
import sqlite3
import time
import numpy as np


class securityManager(object):
    db = "H:\BOND_TRA\ATJ\Projects\GitHub\DatabaseManager\database.db"

    def __init__(self):
        '''

        '''
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()

        self.SecEngine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\GitHub\DatabaseManager\database.db')

    def parseCmdLine(self):
        parser = OptionParser(description="Retrieve reference data.")
        parser.add_option("-a",
                          "--ip",
                          dest="host",
                          help="server name or IP (default: %default)",
                          metavar="ipAddress",
                          default="localhost")
        parser.add_option("-p",
                          dest="port",
                          type="int",
                          help="server port (default: %default)",
                          metavar="tcpPort",
                          default=8194)

        (options, args) = parser.parse_args()

        return options

########################################################################################################################
    def initializeSecDatabase(self):
        '''
        - does db exist
        - does cixsMaster exist
        - does spread table exist
        :return:
        '''
        print('Start initializeSecDatabase()')
        exists = self.doesSecMasterExist()
        if exists is True:
            print("secMaster exists!")
            self.setSecMasterVar()
            pass
        else:
            print("secMaster does not exist!")
            self.createSecMaster()

        print('Finish initializeSecDatabase()')
        pass

########################################################################################################################
# InitializeSecDatabase() Child functions
    # Called by InitializeSecDatabase()
    def doesSecMasterExist(self):
        '''

        :return:
        '''
        cmd = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='secMaster'"
        self.cur.execute(cmd)
        data = self.cur.fetchone()
        exists = data[0]
        if exists == 0:
            exists = False
        else:
            exists = True
        return exists

    # Called by InitializeSecDatabase()
    def createSecMaster(self):
        '''
        :return:
        '''

        df = self.generateSecMasterDataframe()
        df.to_sql("secMaster", con=self.db)
        print(df.head())
        pass

    # Called by createSecMaster()
    # InitializeSecDatabase() --> createSecMaster()
    def generateSecMasterDataframe(self):
        '''
        - Read in excel copy of preliminary secMaster
        --> This will really just be for initial setup
        :return:
        '''
        # TODO - set up excel file to read in as secMaster

        self.setSecMasterVar()
        return df

    # Called by either generateSecMasterDataframe() or initializeSecDatabase()
    def setSecMasterVar(self):
        '''
        Sets dataframe version of the secMaster table as a class variable to minimize the excessive reading from the
        database
        :return:
        '''
        self.SM_df = pd.read_sql_table("secMaster", con=self.SecEngine)
        pass

########################################################################################################################
    # Create new table
    def initializeTable_PX_Last(self):
        '''
        - Does the table exist
        - build the empty table
        - pull the data
        - populate the table
        - insert into database
        - set as class var
        :param tableName:
        :return:
        '''
        '''
        - does db exist
        - does cixsMaster exist
        - does spread table exist
        :return:
        '''
        print('Start initializeTable_PX_Last()')
        exists = self.doesFieldTableExist("PX_LAST")
        if exists is True:
            print("PX_LAST exists!")
            print("Reading PX_LAST into memory...")
            securityManager.PX_LAST_df = pd.read_sql_table("PX_LAST", con=self.SecEngine, index_col='Date', parse_dates={'Date': "%Y-%m-%d"})
            print(self.PX_LAST_df.head())
            pass
        else:
            print("PX_LAST does not exist!")
            self.createTable_PX_LAST()
            df = securityManager.PX_LAST_df
            df.to_sql("PX_LAST", con=self.SecEngine, if_exists='replace', index=True)

        print('Finished initializeTable_PX_Last()')
        pass

########################################################################################################################
# initializeTable_PX_Last() Child Functions
    def doesFieldTableExist(self, tableName):
        '''
        - Test to see if a table with the passed name exists within the database
        :return:
        '''
        # print("doesFieldTableExist() Start")
        cmd = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'" % tableName
        self.cur.execute(cmd)
        data = self.cur.fetchone()
        exists = data[0]
        if exists == 0:
            exists = False
        else:
            exists = True
        # print(exists)
        # print("doesFieldTableExist() Finish")
        return exists

    def createTable_PX_LAST(self):
        '''

        :return:
        '''
        print("createTable_PX_LAST() Start")
        end = dt.date.today().strftime("%Y%m%d")
        idx = pd.date_range("19991231", end, freq="B", name="Date")

        cols = self.SM_df.SecID
        # print(cols)

        df = pd.DataFrame(index=idx, columns=cols)
        # print(df.head())

        print('Start')
        cols = cols.tolist()
        for col in cols:
            print(col)
            start = time.time()
            temp = self.getBBGData(col, "DAILY", "PX_LAST")
            temp.set_index("date", drop=True, inplace=True)
            # print(temp.head())

            # print(len(temp))
            # print(len(df.index))
            df = pd.concat([df, temp], axis=1, join_axes=[df.index])
            stop = time.time()
            total = stop - start
            print(total)

        # print(df.head())
        securityManager.PX_LAST_df = df
        print("createTable_PX_LAST() Finish")
        return

########################################################################################################################
# createTable_XXX() Child Functions
    # called by createTable_PX_LAST()
    def getBBGData(self, sec, freq, field):

        options = self.parseCmdLine()

        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(options.host)
        sessionOptions.setServerPort(options.port)

        print("Connecting to %s:%s" % (options.host, options.port))
        # Create a Session
        session = blpapi.Session(sessionOptions)

        # Start a Session
        if not session.start():
            print("Failed to start session.")
            # return
        else:
            print('Session Started')

        try:
            # Open service to get historical data from
            if not session.openService("//blp/refdata"):
                print("Failed to open //blp/refdata")
                # return
                # Obtain previously opened service
        except UnboundLocalError as ULE:
            print(ULE)
            pass

        refDataService = session.getService("//blp/refdata")

        sd = "19991231"
        ed = dt.date.today().strftime("%Y%m%d")

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")

        request.getElement("securities").appendValue(str(sec) + " INDEX")

        request.getElement('fields').appendValue(field)

        # request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", freq)
        request.set("startDate", sd)
        request.set("endDate", ed)
        # request.set("maxDataPoints", 100)

        # Send the request
        session.sendRequest(request)

        # Process received events
        holdFrame = self.processMessage(session, sec, field)

        # print(holdFrame.head())
        holdFrame.drop(labels="secID", axis=1, inplace=True)
        holdFrame.rename(columns={"PX_LAST": sec}, inplace=True)
        return holdFrame

    def processMessage(self, session, sec, field):
        # Set various variables equal to various features within BLPAPI
        # These "names" references keys with the JSON that is returned by BBG after a call
        SECURITY_DATA = blpapi.Name("securityData")
        SECURITY = blpapi.Name("security")
        FIELD_DATA = blpapi.Name("fieldData")
        FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
        FIELD_ID = blpapi.Name("fieldId")
        ERROR_INFO = blpapi.Name("errorInfo")

        while (True):
            ev = session.nextEvent(500)
            for msg in ev:
                # print(msg)
                if ev.eventType() == blpapi.Event.PARTIAL_RESPONSE or ev.eventType() == blpapi.Event.RESPONSE:
                    secName = msg.getElement(SECURITY_DATA).getElementAsString(SECURITY)
                    fieldDataArray = msg.getElement(SECURITY_DATA).getElement(FIELD_DATA)
                    size = fieldDataArray.numValues()
                    fieldDataList = [fieldDataArray.getValueAsElement(i) for i in range(0, size)]
                    outDates = [x.getElementAsDatetime('date') for x in fieldDataList]
                    dateFrame = pd.DataFrame({'date': outDates})
                    strData = [field]
                    output = pd.DataFrame(columns=strData)
                    for strD in strData:
                        outData = [x.getElementAsFloat(strD) for x in fieldDataList]
                        output[strD] = outData
                        output['secID'] = secName
                        output = pd.concat([output], axis=1)
                    output = pd.concat([dateFrame, output], axis=1)
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        return output


########################################################################################################################


    def update_Sec_Master(self):
        '''
        Ensure that each field end date is updated with the most recent date
        create tables needs to be run first
        :return:
        '''

        """
        select all tables on sec master
        create list of tables
        for each table select fields
        for each field find max date
        find table and field on sec master
        update end date with max date if different
        """

        self.cur.execute("SELECT secMaster.SecID FROM secMaster")
        tupleList = self.cur.fetchall()
        tableList = []
        for i in tupleList:
            tableList.append(i[0])

        print(tableList)

        for table in tableList:
            fieldList = []
            cmd = "PRAGMA table_info(%s)" % table
            self.cur.execute(cmd)
            tempCMD = self.cur.fetchall()
            # print(tempCMD)
            # print(table)
            fieldList = [x[1] for x in tempCMD]
            fieldList.remove('date')
            fieldList.remove('id')
            # print(fieldList)
            for f in fieldList:
                # print(f)
                cmd2 = "SELECT MAX(date) FROM %s WHERE %s IS NOT NULL" % (table, f)
                self.cur.execute(cmd2)
                tempCMD2 = self.cur.fetchall()
                # print(tempCMD2)
                # print("!!")
                for i in tempCMD2:
                    # print(i)
                    # print('!')
                    if i[0] != None:
                        # print("Yo!")
                        fieldNum = self.find_field_name(table, f)
                        # print(i[0] + "!!")
                        self.update_End_Date(fieldNum, table, str(i[0]).replace("-", ""))

        return

    def find_field_name(self, table, f):
        self.cur.execute("SELECT * FROM secMaster WHERE secID LIKE (?)", (table,))
        des = self.cur.description
        query = self.cur.fetchall()
        fieldPos = list(query[0]).index(f)
        colList = [y for y in des]
        colName = colList[fieldPos][0].split("_")[0]
        # print(colName + "!")
        fieldNum = colName[-1]
        # print(fieldNum)
        return fieldNum

    def update_End_Date(self, fieldNum, sec, date):
        field = "Field%s_ED" % fieldNum
        # print(field)
        # print(date)
        cmd = "UPDATE secMaster SET %s = '%s' WHERE secID LIKE '%s'" % (field, str(date), sec)
        # print(cmd)
        self.cur.execute(cmd)
        self.conn.commit()
        print(sec + " " + field + " Updated!")
        return

    def check_if_col_is_empty(self, table, col):
        cmd = "SELECT * FROM %s WHERE %s NOT NULL" % (table, col)
        self.cur.execute(cmd)
        data = self.cur.fetchall()
        # print(data)
        if not data:
            status = "Empty"
        else:
            status = "Active"
        return status



    def continue_with_call(self, sec, field):
        cmd = "SELECT MAX(date) FROM %s WHERE %s NOT NULL" % (sec.SecID, field)
        self.cur.execute(cmd)
        ans = self.cur.fetchall()
        # print(ans[0][0])
        date = dt.date.today()
        # print(date)
        if ans[0][0] == str(date):
            # print('Most recent date is today')
            return True
        else:
            # print('Most recent date not today')
            date -= dt.timedelta(days=1)
            while date.weekday() > 4:
                date -= dt.timedelta(days=1)
            # print(str(date) + "-continue")
            if ans[0][0] == str(date):
                # print('Most recent date equal previous business day')
                return True
            else:
                # print('Most recent date not equal to previous business ')
                return False

    def __repr__(self):
        pass

    def __str__(self):
        pass


class cixsManager(securityManager):
    CIXS_db = "H:\BOND_TRA\ATJ\Projects\GitHub\DatabaseManager\CIXS.db"

    def __init__(self):
        self.conn = sqlite3.connect(self.CIXS_db)
        self.cur = self.conn.cursor()

        # self.ShortList = self.getShortList()
        # self.FullList = self.getFullList()

        self.SecEngine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\GitHub\DatabaseManager\database.db')
        self.CIXSEngine = create_engine(r'sqlite:///H:\BOND_TRA\ATJ\Projects\GitHub\DatabaseManager\CIXS.db')

    def initializeCIXSDatabase(self):
        '''
        - does db exist
        - does cixsMaster exist
        - does spread table exist
        :return:
        '''
        print('Start initializeCIXSDatabase()')
        exists = self.doesCIXSMasterExist()
        if exists is True:
            print("cixsMaster exists!")
            self.setCIXSMasterVar()
            pass
        else:
            print("cixsMaster does not exist!")
            self.createCIXSMaster()

        print('Finish initializeCIXSDatabase()')
        pass

    def initializeTable_Spread(self):
        '''
        - Does the table exist
        - build the empty table
        - pull the data
        - populate the table
        - insert into database
        - set as class var
        :param tableName:
        :return:
        '''
        '''
        - does db exist
        - does cixsMaster exist
        - does spread table exist
        :return:
        '''
        print('Start initializeTable_Spread()')
        exists = self.doesFieldTableExist("Spread")
        if exists is True:
            print("Spread exists!")
            print("Reading Spread into memory...")
            self.Spread_df = pd.read_sql_table("Spread", con=self.CIXSEngine, index_col='Date', parse_dates={'Date': "%Y-%m-%d"})
            print(self.Spread_df.head())
            pass
        else:
            print("Spread does not exist!")
            self.buildSpreadTable()
            df = self.Spread_df
            df.to_sql("Spread", con=self.CIXSEngine, if_exists='replace', index=True)

        print('Finished initializeTable_Spread()')
        pass

    def initializeTable_StdDev(self, period):
        '''

        I can initialize multiple of these by throwing the periods into a list and using a for loop
        :param period:
        :return:
        '''
        print('Start initializeTable_StdDev()', period)
        tableName = "StdDev_" + str(period)
        exists = self.doesFieldTableExist("PX_LAST")
        if exists is True:
            print(tableName + " exists!")
            print("Reading PX_LAST into memory...")
            df = pd.read_sql_table(tableName, con=self.CIXSEngine, index_col='Date', parse_dates={'Date': "%Y-%m-%d"})
            print(df.head())
            pass
        else:
            print(tableName + " does not exist!")
            df = self.buildStdDevTable(period)
            df.to_sql(tableName, con=self.CIXSEngine, if_exists='replace', index=True)

        print('Finished initializeTable_StdDev()')
        pass

    def updateFieldTable(self):
        '''
        - Can request to update this table but cannot guarantee that all the necessary data will be there
            -- ex trying to update zscore table without std dev table having all of the necessary dates

        :return:
        '''
        # TODO - solve updating problem
        pass

    def updateAllTables(self):
        '''

        :return:
        '''
        pass

    ########################################################################################################################
    # InitializeCIXSDatabase() Child functions
    # Called by InitializeCIXSDatabase()
    def doesCIXSMasterExist(self):
        '''

        :return:
        '''
        cmd = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='cixsMaster'"
        self.cur.execute(cmd)
        data = self.cur.fetchone()
        exists = data[0]
        if exists == 0:
            exists = False
        else:
            exists = True
        return exists

    # Called by InitializeCIXSDatabase()
    def createCIXSMaster(self):
        '''
        - Create if not exists
        - Cols
        -- cixsID
        -- name
        -- shortName
        -- longLeg
        -- shortLeg
        -- longTenor
        -- shortTenor
        -- fields

        :return:
        '''

        df = self.generateCIXSMasterDataframe()
        df.to_sql("cixsMaster", con=self.CIXSEngine)
        print(df.head())
        pass

    # Called by createCIXSMaster()
    # InitializeCIXSDatabase() --> createCIXSMaster()
    def generateCIXSMasterDataframe(self):
        '''
        - read secMaster from secDB
        - filter
        -- Tenor
        -- CIXS Active
        -- Other?
        - perform combinations
        - populate other cols
        :return:
        '''
        # Specifies the tenor of the indices we want to use to build CIXS
        tenorList = [3, 5, 7, 10, 15, 20, 30]

        secMaster = pd.read_sql_table("secMaster", con=self.SecEngine)
        secMaster = secMaster[(secMaster['CIXS_Active'] == 'Y') &
                              (secMaster['Tenor_Num'].isin(tenorList))]

        shortList = secMaster.loc[(secMaster['Shortable'] == 'Y')]['SecID'].tolist()

        fullList = secMaster['SecID'].tolist()
        df = pd.DataFrame()
        cixsList = [(short + "_" + long) for short in shortList for long in fullList if short != long]
        df['CIXS_ID'] = cixsList
        df['ShortLeg'] = df['CIXS_ID'].str.split('_', expand=True)[0]
        df['LongLeg'] = df['CIXS_ID'].str.split('_', expand=True)[1]
        df['Active'] = 'Y'

        # Isolate the two columns we want to join
        temp = secMaster[['SecID', 'Tenor_Num']]

        # Reset the index so that the 'SecID' column is used as the key
        temp.set_index('SecID', drop=True, inplace=True)

        df = df.join(temp, on='LongLeg')
        df.rename(columns={"Tenor_Num": "LongLeg_Tenor"}, inplace=True)

        # Isolate the two columns we want to join
        temp = secMaster[['SecID', 'Tenor_Num']]

        # Reset the index so that the 'SecID' column is used as the key
        temp.set_index('SecID', drop=True, inplace=True)

        df = df.join(temp, on='ShortLeg')
        df.rename(columns={"Tenor_Num": "ShortLeg_Tenor"}, inplace=True)
        self.setCIXSMasterVar()
        return df

    def setCIXSMasterVar(self):
        self.CM_df = pd.read_sql_table("cixsMaster", con=self.CIXSEngine)
        pass

    ########################################################################################################################
    # Table Generating Parent Functions
    # Called by initializeTable_Spread()
    def buildSpreadTable(self):
        '''
        - index is dates
        - cols: cixsID
        - values: spreads (long - short)
        - need to get dates to create as index
        :return:
        '''
        end = dt.date.today().strftime("%Y%m%d")
        idx = pd.date_range("19991231", end, freq="B", name="Date")

        cols = self.CM_df.CIXS_ID
        # print(cols)

        df = pd.DataFrame(index=idx)
        # print(df.head())
        print('Start')
        cols = cols.tolist()
        df_list = []
        start = time.time()
        for i in cols[0:100]:
            print(i)
            temp = self.getSpreadData(idx, i)
            df_list.append(temp)
            # print(len(temp))
            # print(len(df.index))

        df = pd.concat(df_list, axis=1, join_axes=[df.index])
        stop = time.time()
        total = stop - start
        print(total)

        print(df.head())
        # TODO add appropriate interpolation and fillna functions
        self.Spread_df = df
        print('Final')
        pass

    # Called by initializeTable_StdDev():
    def buildStdDevTable(self, period):
        end = dt.date.today().strftime("%Y%m%d")
        idx = pd.date_range("19991231", end, freq="B", name="Date")

        cols = self.CM_df.CIXS_ID
        # print(cols)

        df = pd.DataFrame(index=idx, columns=cols)
        df_list = []
        start = time.time()
        for col in cols[0:20]:
            print(col)
            temp = self.getStdDev(idx, col, period)
            df_list.append(temp)
            # print(len(temp))
            # print(len(df.index))

            df = pd.concat(df_list, axis=1, join_axes=[df.index])

        df = pd.concat(df_list, axis=1, join_axes=[df.index])
        stop = time.time()
        total = stop - start
        print(total)

        print(df)
        exit()
        return df

    def buildCorrTable(self):
        end = dt.date.today().strftime("%Y%m%d")
        idx = pd.date_range("19991231", end, freq="B", name="Date")

        cols = self.CM_df.CIXS_ID
        print(cols)

        df = pd.DataFrame(index=idx, columns=cols)
        print(df.head())
        pass

    def buildZScoreTable(self):
        end = dt.date.today().strftime("%Y%m%d")
        idx = pd.date_range("19991231", end, freq="B", name="Date")

        cols = self.CM_df.CIXS_ID
        print(cols)

        df = pd.DataFrame(index=idx, columns=cols)
        print(df.head())
        pass

    ########################################################################################################################
    # Table Generating Child Functions

    def getSpreadData(self, dates, cixs):
        '''
        ***To run this function, the intitializeTable_PX_LAST() function must be run. The PX_LAST_df class variable must be set***
        - use cixsMaster to retreive yield anc calc spreads
        - return df that will me merged/joined on the date index
            - This is a quirk - because it is easiest to just retreive consecutive dates from bbg and filter for business days later
            - Will interpolate to fill any further holes
        :return:
        '''
        # TODO - update the df without re calcing everything
        shortLeg = cixs.split("_")[0]
        longLeg = cixs.split("_")[1]
        start = time.time()
        shortSeries = securityManager.PX_LAST_df[shortLeg]
        shortDF = pd.DataFrame(data=shortSeries.values, index=shortSeries.index)
        stop = time.time()
        total = stop - start
        # print(total)
        shortDF.rename(columns={shortDF.columns[0]: shortLeg}, inplace=True)
        shortDF = shortDF[~shortDF.index.duplicated(keep='first')]

        longSeries = securityManager.PX_LAST_df[longLeg]
        longDF = pd.DataFrame(data=longSeries.values, index=longSeries.index)
        longDF.rename(columns={longDF.columns[0]: longLeg}, inplace=True)
        longDF = longDF[~longDF.index.duplicated(keep='first')]

        # print(shortDF.head())
        # print(longDF.head())

        tempDF = pd.DataFrame(index=dates)

        tempDF1 = pd.merge(tempDF, shortDF, how='left', left_index=True, right_index=True)
        tempDF2 = pd.merge(tempDF, longDF, how='left', left_index=True, right_index=True)
        tempDF3 = pd.merge(tempDF1, tempDF2, how='left', left_index=True, right_index=True)
        tempDF3[cixs] = tempDF3[longLeg] - tempDF3[shortLeg]
        tempDF3 = tempDF3[cixs]
        # print(tempDF3.head())
        return tempDF3

    def getStdDev(self, dates, cixs, period):
        '''

        :param dates:
        :param cixs:
        :param period:
        :return:
        '''
        stdDF = self.Spread_df[cixs].rolling(window=period, min_periods=(period - 10).).std()
        stdDF = pd.DataFrame(data=stdDF.values, index=stdDF.index)
        stdDF.rename(columns={stdDF.columns[0]: cixs}, inplace=True)
        tempDF = pd.DataFrame(index=dates)
        tempDF1 = pd.merge(tempDF, stdDF, how='left', left_index=True, right_index=True)
        return tempDF1


"""
Security object

Security Database

CIXS Database


General Workflows
- Initialize a database
    - Create a master
        -- This should be dependent on the shortable list and the long list
        -- Should be dependent on the active securities on the securityMaster
        -- Should be based on the tenor 3, 5, 10, 15, 20, 30
    - Create tables for each field
        -- Only create tables for CIXS marked as active
    - Populate those tables with data
        -- Start w/ basic spread data
- Add fields to objects
    -- for each table, read to df add column with desired field, set equal to some calc
    - Update tables with fields
    - Populate those tables with necessary data
- Update all tables
    - Check last time updated
    - Add missing data
- Update single table

"""

sm = securityManager()
sm.initializeSecDatabase()
sm.initializeTable_PX_Last()
cm = cixsManager()
cm.initializeCIXSDatabase()
cm.initializeTable_Spread()
cm.initializeTable_StdDev(30)
