import pandas as pd
#import blpapi
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


    def create_new_table(self, security):
        '''

        :param security:
        :return:
        '''
        # print("Create new table")
        new_table = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, " % (security.SecID)
        colList = []
        d = sec.Fields
        for key1 in d.keys():
            for key2 in d[key1]:
                if key2 == "Name":
                    val = d[key1][key2]
                    colList.append(val)

        for f in colList:
            st = f + " REAL, "
            new_table += st

        new_table = new_table[:-2]
        new_table = new_table + ")"
        # print(new_table)
        self.cur.execute(new_table)

        return

    # need to add a variable to pass the list of fields that need to be added
    def create_new_field(self, security):
        '''

        :return:
        '''
        # print("Create new field")
        tableName = security.SecID
        field = 'llnmvsdf'
        new_field = "ALTER TABLE " + tableName + " ADD COLUMN " + field + "'float'"
        self.cur.execute(str(new_field))
        return


    def does_table_exist(self, security):
        '''
        Check if a given security exists within the database. If it doesn't, call the create_new_table
        function and pass the security as an argument

        :param security:
        :return:
        '''
        tableName = security.SecID
        self.cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=(?)", (tableName,))
        data = self.cur.fetchone()
        exists = data[0]
        if exists == 0:
            self.create_new_table(security)
            exists = 'Table Does Not Exist! New Table Created!'
            # print(exists)
        else:
            exists = 'Table Already Exists!'
            # print(exists)
        return exists


    def does_field_exist(self, security):
        '''
        Check if a given security has a specific field. If not, add that field to the table.
        :return:
        '''
        tableName = security.SecID

        self.cur.execute("SELECT ? FROM sqlite_master", (tableName,))
        data = self.cur.fetchall()
        # for i in data:
        #     print(i)

        # here I need to run through thr gammut of finding all the field names specified by the sec master
        field = "null"
        l = []
        code = "PRAGMA table_info(" + tableName + ")"
        self.cur.execute(str(code))
        data1 = self.cur.fetchall()
        for i in data1:
            l.append(i[1])

        if field not in l:
            self.create_new_field(security)
            exists = 'Field Does Not Exist! New Field Created!'
        else:
            exists = 'Field Already Exists!'
        # print(exists)
        return exists


    def create_Sec_Master(self, tableDF):
        try:
            tableDF.to_sql("secMaster", con=self.conn, index=False, flavor='sqlite', if_exists="fail")
            print("secMaster was created!")
        except ValueError as VE:
            print("secMaster already exists!")
        return


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


    def delete_duplicate_rows(self, sec, field):
        tableName = sec.SecID
        cmd = "DELETE FROM %s WHERE id NOT IN (SELECT MIN(id) FROM %s GROUP BY date, %s)" % (tableName, tableName, field)
        self.cur.execute(cmd)


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

    def createFieldTable(self, tableName):
        '''
        - take field name as argument
        - create table with date as index, col w/ unique record id, and list of active cixs
        - populate table
        -- data cleaning process should be part of this
        --- interpolate missing values
        :return:
        '''
        pass

    def updateFieldTable(self):
        '''
        - Can request to update this table but cannot guarantee that all the necessary data will be there
            -- ex trying to update zscore table without std dev table having all of the necessary dates

        :return:
        '''
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
        self.CM_df = pd.read_sql_table("cixsMaster", con=self.CIXSEngine, )
        pass
########################################################################################################################
# Table Generating Parent Functions
# Called by createFieldTable()

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
        print(cols)

        df = pd.DataFrame(index=idx)
        print(df.head())
        print('Start')
        cols = cols.tolist()
        for i in cols[0:20]:
            print(i)
            start = time.time()
            temp = self.getSpreadData(idx, i)
            # print(len(temp))
            # print(len(df.index))
            df = pd.concat([df, temp], axis=1, join_axes=[df.index])
            stop = time.time()
            total = stop - start
            print(total)

        print(df.head())
        print('Final')
        pass

    def buildStdDevTable(self):
        end = dt.date.today().strftime("%Y%m%d")
        idx = pd.date_range("19991231", end, freq="B", name="Date")

        cols = self.CM_df.CIXS_ID
        print(cols)

        df = pd.DataFrame(index=idx, columns=cols)
        print(df.head())
        pass

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
        shortDF = pd.read_sql_table(shortLeg, con=self.SecEngine, index_col='date', parse_dates={'date': {'format': '%Y-%m-%d'}})
        stop = time.time()
        total = stop - start
        print(total)
        shortDF.rename(columns={"PX_LAST": shortLeg}, inplace=True)
        shortDF.drop(labels='id', axis=1, inplace=True)
        shortDF = shortDF[~shortDF.index.duplicated(keep='first')]

        longDF = pd.read_sql_table(longLeg, con=self.SecEngine, index_col='date', parse_dates={'date': {'format': '%Y-%m-%d'}})
        longDF.rename(columns={"PX_LAST": longLeg}, inplace=True)
        longDF.drop(labels='id', axis=1, inplace=True)
        longDF = longDF[~longDF.index.duplicated(keep='first')]


        tempDF = pd.DataFrame(index=dates)

        tempDF1 = pd.merge(tempDF, shortDF, how='left', left_index=True, right_index=True)
        tempDF2 = pd.merge(tempDF, longDF, how='left', left_index=True, right_index=True)
        tempDF3 = pd.merge(tempDF1, tempDF2, how='left', left_index=True, right_index=True)
        tempDF3[cixs] = tempDF3[longLeg] - tempDF3[shortLeg]
        tempDF3 = tempDF3[cixs]

        return tempDF3



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

cm = cixsManager()
cm.initializeCIXSDatabase()
cm.buildSpreadTable()
