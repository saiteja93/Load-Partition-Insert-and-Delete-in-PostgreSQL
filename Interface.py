#!/usr/bin/python2.7
#
# Interface for the assignement
#Author : Saiteja Sirikonda
#ASU_ID : 1211246826
#Assignment_1 DD
import psycopg2

DATABASE_NAME = 'test_dds_assgn1'
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
metadata = 'Metadata'


def getopenconnection(user='postgres', password='1234', dbname='test_dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'") 


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    global RATINGS_TABLE
    RATINGS_TABLE = ratingstablename
    cur.execute('CREATE TABLE IF NOT EXISTS {}(UserID int, MovieID int, Rating float)'.format(RATINGS_TABLE))
    cur.execute('DELETE FROM {}'.format(RATINGS_TABLE))
    f = open(ratingsfilepath, 'rb')
    content = f.readlines()
    for i in content:
        l = i.split('::')
        u = l[0]
        m = l[1]
        r = l[2]
        cur.execute("INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})".format(RATINGS_TABLE,u,m,r))
    cur.close()


    #create a Table called Ratings 


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cur = openconnection.cursor()
        global RANGE_TABLE_PREFIX
        RATINGS_TABLE_PREFIX = ratingstablename
        global NofRangePartitions
        NofRangePartitions = numberofpartitions
        cur.execute('UPDATE {} SET NofRangePartitions = {}'.format(metadata,numberofpartitions))
        cur.execute("SELECT * from {}". format(RATINGS_TABLE))
        rows = cur.fetchall()
        if NofRangePartitions==0:
            print "No partitions entered"
        i = 0
        while(i<NofRangePartitions):
            cur.execute('CREATE TABLE IF NOT EXISTS {} (UserID int, MovieID int, Rating int)'.format(RANGE_TABLE_PREFIX + str(i)))
            cur.execute('DELETE FROM {}'.format(RANGE_TABLE_PREFIX + str(i)))
            i = i+1
        divisor = (5.0/NofRangePartitions)
        bound = round(divisor,2)
        parti = []
        # while True:
        #     end = k*bound
        #     if end <= 5:
        #         k = k + 1
        #         parti.append(end)
        #         #print parti
        #     else: 
        #         break
        for i in range(1, NofRangePartitions):
            end = i*bound
            parti.append(end)

        print "perti is : "+str(parti)

        for row in rows:
            ratin = row[2]
            #print ratin
            l = len(parti)
            k = 0
            while True:
                if (ratin <= parti[l-1]):
                    if ratin== parti[k]:
                        #print (k+2)
                        #insert into partition table number (k+2)
                        cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RANGE_TABLE_PREFIX+str(k+1),row[0],row[1],row[2]))
                        break
                    elif(ratin < parti[k]):
                        #print (k+1)
                        #insert into partition table number (k+1)
                        cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RANGE_TABLE_PREFIX+str(k),row[0],row[1],row[2]))
                        break
                    else:
                        k = k+1
                else:
                    #print l+1
                    #insert into partition table number (l+1)
                    cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RANGE_TABLE_PREFIX+str(l),row[0],row[1],row[2]))
                    break

        #how do you access a particular field

        cur.close() #check if this is right?
    except Exception as E:
        print E


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cur = openconnection.cursor()
        global RROBIN_TABLE_PREFIX
        global NofRoundRobinPartitions
        NofRoundRobinPartitions =  numberofpartitions

        cur.execute("SELECT * from {}". format(RATINGS_TABLE))
        rows = cur.fetchall()
        if NofRoundRobinPartitions==0:
            print "No partitions entered"
        i = 0
        while(i<NofRoundRobinPartitions):
            cur.execute('CREATE TABLE IF NOT EXISTS {}(UserID int, MovieID int, Rating int)'.format(RROBIN_TABLE_PREFIX + str(i)))
            cur.execute('DELETE FROM {}'.format(RROBIN_TABLE_PREFIX + str(i)))
            i=i+1
        #cur.execute('CREATE TABLE IF NOT EXISTS {} (last_table_index int)'. format(metadata))
        k = 0
        for row in rows:
            i = k % NofRoundRobinPartitions
            cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RROBIN_TABLE_PREFIX+str(i),row[0],row[1],row[2]))
            k = k+1
        last_index = i
        cur.execute('UPDATE {} SET last_table_index = {}'.format(metadata,last_index))
        cur.execute('UPDATE {} SET NofRoundRobinPartitions = {}'.format(metadata,numberofpartitions))
        

        cur.close()
    except Exception as E:
        print E


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()
        cur.execute('SELECT * from {}'.format(metadata))
        val = cur.fetchall()[-1]
        last_index = val[0]
        N = val[2]
        print last_index,N
        cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RROBIN_TABLE_PREFIX+str((last_index+1)%N),userid,itemid,rating))
        cur.execute('UPDATE {} SET last_table_index = {}'.format(metadata,(last_index+1)%N))
        cur.close()
            ### How to import N to this function
    except Exception as E:
        print E





def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cur = openconnection.cursor()
        cur.execute('SELECT * from {}'.format(metadata))
        val= cur.fetchall()[-1]
        NofRangePartitions = val[1]
        divisor = (5.0/NofRangePartitions)
        bound = round(divisor,2)
        parti = []
        for i in range(1, NofRangePartitions):
            end = i*bound
            parti.append(end)
        l = len(parti)
        k = 0
               
        while True:
            if (rating <= parti[-1]):
                if rating== parti[k]:
                    #print (k+1)
                    #insert into partition table number (k+1)
                    cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RANGE_TABLE_PREFIX+str(k),userid,itemid,rating))
                    break
                elif(rating < parti[k]):
                    #print (k+1)
                    #insert into partition table number (k+1)
                    cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RANGE_TABLE_PREFIX+str(k),userid,itemid,rating))
                    break
                else:
                    k = k+1
            else:
                #print l+1
                #insert into partition table number (l+1)
                cur.execute('INSERT INTO {} (UserID,MovieID,Rating) VALUES ({},{},{})'.format(RANGE_TABLE_PREFIX+str(l),userid,itemid,rating))
                break
        cur.close()
    except Exception as E:
        print E
    




def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute('DROP SCHEMA {} CASCADE'.format(DATABASE_NAME))
    cur.execute('CREATE SCHEMA {}'.format(DATABASE_NAME))
    cur.close()

# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    cur = openconnection.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS {} (last_table_index int, NofRangePartitions int, NofRoundRobinPartitions int)'.format(metadata))
    #cur.execute('DELETE FROM {}'.format(metadata))
    cur.execute('INSERT INTO {} (last_table_index,NofRangePartitions,NofRoundRobinPartitions) VALUES ({},{},{})'.format(metadata,0,0,0))
    cur.close()


def after_test_script_ends_middleware(openconnection, databasename):
    #
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(test_dds_assgn1)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
