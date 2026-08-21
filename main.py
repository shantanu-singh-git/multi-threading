"""
This python module utilizes multithreading to push data into separate tables in
a database
"""

import pandas as pd
from sqlalchemy import create_engine
from threading import Thread as Th

"""
Note: Following tasks remain to be done: 
    1. Create utils and config files to hide connection string credentials
    2. Add logging
    3. Use timeit to map difference between sequential and parallel execution
    workflow time consumption over 100,000 loops
    4. Increase chunksize parameter automatically 
    if the original dataframe is large

Also note: conn formal argument / engine actual argument is an object, but
pycharm gives a warning, hence conn type is connection inside function docstring
"""

def push_into_it(frame, conn) -> None:
    """
    Inserts dataframe data into IT table
    :param frame: Input dataframe to be pushed into the table
    :type frame: pandas.DataFrame
    :param conn: Connection string to be pushed into the table
    :type conn: Connection
    """
    try:
        a = frame.to_sql("it", conn, if_exists="append", index=False,
                         chunksize=25)
        print(f"{a} rows inserted into IT table successfully")
        del frame, conn, a
    except Exception as e:
        raise e

def push_into_hr(frame, conn) -> None:
    """
    Inserts dataframe data into HR table
    :param frame: Input dataframe to be pushed into the table
    :type frame: pandas.DataFrame
    :param conn: Connection string to be pushed into the table
    :type conn: Connection
    """
    try:
        a = frame.to_sql("hr", conn, if_exists="append", index=False,
                         chunksize=25)
        print(f"{a} rows inserted into HR table successfully")
        del frame, conn, a
    except Exception as e:
        raise e

def push_into_admin(frame, conn) -> None:
    """
    Inserts dataframe data into Admin table
    :param frame: Input dataframe to be pushed into the table
    :type frame: pandas.DataFrame
    :param conn: Connection string to be pushed into the table
    :type conn: Connection
    """
    try:
        a=frame.to_sql("admin", conn, if_exists="append", index=False,
                       chunksize=25)
        print(f"{a} rows inserted into Admin table successfully")
        del frame, conn, a
    except Exception as e:
        raise e

def push_into_sales(frame, conn) -> None:
    """
    Inserts dataframe data into Sales table
    :param frame: Input dataframe to be pushed into the table
    :type frame: pandas.DataFrame
    :param conn: Connection string to be pushed into the table
    :type conn: Connection
    """
    try:
        a= frame.to_sql("sales", conn, if_exists="append", index=False,
                        chunksize=25)
        print(f"{a} rows inserted into Sales table successfully")
        del frame, conn, a
    except Exception as e:
        raise e

def push_into_customer_support(frame, conn) -> None:
    """
    Inserts dataframe data into Customer Support table
    :param frame: Input dataframe to be pushed into the table
    :type frame: pandas.DataFrame
    :param conn: Connection string to be pushed into the table
    :type conn: Connection
    """
    try:
        a = frame.to_sql("customer_support", conn,
                     if_exists="append", index=False, chunksize=25)
        print(f"{a} rows inserted into Customer Support table successfully")
        del frame, conn,a
    except Exception as e:
        raise e

def push_into_finance(frame, conn) -> None:
    """
    Inserts dataframe data into Finance table
    :param frame: Input dataframe to be pushed into the table
    :type frame: pandas.DataFrame
    :param conn: Connection string to be pushed into the table
    :type conn: Connection
    """
    try:
        a = frame.to_sql("finance", conn, if_exists="append",
                         index=False, chunksize=25)
        print(f"{a} rows inserted into Finance table successfully")
        del frame, conn, a
    except Exception as e:
        raise e


if __name__ == '__main__':

    df = pd.read_csv('employees.csv')

    #split into department wise dataframes
    it = df[df["department"] == "IT"]
    hr = df[df["department"] == "HR"]
    sales = df[df["department"] == "Sales"]
    admin = df[df["department"] == "Admin"]
    cust_support = df[df["department"] == "Customer Support"]
    finance = df[df["department"] == "Finance"]

    del df

    engine = create_engine(
        "mysql+pymysql://root:rootpass1234@localhost/multithread")

    #define the threads
    t1 = Th(target=push_into_it, args=(it, engine))
    t2 = Th(target=push_into_finance, args=(finance, engine))
    t3 = Th(target=push_into_hr, args=(hr, engine))
    t4 = Th(target=push_into_sales, args=(sales, engine))
    t5 = Th(target=push_into_customer_support, args=(cust_support, engine))
    t6 = Th(target=push_into_admin, args=(admin, engine))

    #initialize parallel data dumping
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()

    del it, hr, sales, admin, cust_support, finance

    print("All the data has been successfully transferred into the respective "
    "tables")
