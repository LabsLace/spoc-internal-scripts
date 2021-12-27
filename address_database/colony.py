import psycopg2
from psycopg2 import Error
import unidecode
import pandas as pd
import xlrd
import os


xls = xlrd.open_workbook(r'CPdescarga.xls', on_demand=True)
sheet_names = xls.sheet_names()
del sheet_names[0]
sheet_names

# Connect excel and sheet file
for sheet in sheet_names:
    try:
        connection = psycopg2.connect(
            host="localhost",
            port="5432",
            database="postgres",
            user="postgres",
            password="cbi2132015379"
        )

        cursor = connection.cursor()
        # Executing a SQL query to insert data into  table
        insert_query = """ SELECT id, municipality FROM municipality_{}""".format(
            sheet)
        cursor.execute(insert_query)
        result = cursor.fetchall()
        #print(result)

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    df_municipality = pd.DataFrame(result, columns=["id", "municipality"])

    # Connect excel and sheet file
    colony_df = pd.DataFrame()
    df_sheet = pd.read_excel('CPdescarga.xls', sheet)
    df_sheet = df_sheet[['d_asenta', 'D_mnpio']]
    df_sheet = df_sheet.drop_duplicates()
    colony_df = colony_df.append(df_sheet)

    test = colony_df.join(df_municipality.set_index(
        'municipality'), on='D_mnpio')
    colony = test[['d_asenta', 'id']]
    colony_list = colony.values.tolist()

    try:
        connection = psycopg2.connect(
            host="localhost",
            port="5432",
            database="postgres",
            user="postgres",
            password="cbi2132015379"
        )

        cursor = connection.cursor()
        # SQL query to create a new table
        create_table_query = '''
                CREATE TABLE public.colony_{} (
                    id serial4 NOT NULL,
                    colony varchar(60) NOT NULL,
                    id_municipality int4 NOT NULL
                );
                  '''.format(sheet)
        # Execute a command: this creates a new table
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    try:

        connection = psycopg2.connect(
            host="localhost",
            port="5432",
            database="postgres",
            user="postgres",
            password="cbi2132015379"
        )
        cursor = connection.cursor()
        q = "insert into colony_{}(colony,id_municipality)values(%s,%s)".format(
            sheet)
        cursor.executemany(q, colony_list)
        connection.commit()
        print("Table updated successfully in PostgreSQL ")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


""" Merge databases """

colony = pd.DataFrame()
# Connect excel and sheet file
for sheet in sheet_names:
    try:
        connection = psycopg2.connect(
            host="localhost",
            port="5432",
            database="postgres",
            user="postgres",
            password="cbi2132015379"
        )

        cursor = connection.cursor()
        # Executing a SQL query to insert data into  table
        insert_query = """ SELECT colony,id_municipality FROM colony_{}; """.format(
            sheet)
        cursor.execute(insert_query)
        result = cursor.fetchall()
        insert_query = """ SELECT id_state FROM municipality_{}; """.format(
            sheet)
        cursor.execute(insert_query)
        result2 = cursor.fetchall()
        #print(result)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    state_df = pd.DataFrame(result2, columns=["id_state"])
    colony_df = pd.DataFrame(result, columns=["colony", "id_municipality"])
    colony_df['id_state'] = state_df
    colony_df = colony_df[["colony", "id_municipality", "id_state"]]
    colony_df = colony_df.fillna(colony_df['id_state'][0])

    print(sheet)
    colony = colony.append(colony_df)
colony_list = colony.values.tolist()


try:
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="cbi2132015379"
    )

    cursor = connection.cursor()
    # SQL query to create a new table
    create_table_query = '''
            CREATE TABLE public.colony (
                id serial4 NOT NULL,
                colony varchar(60) NOT NULL,
                id_municipality int4 NOT NULL,
                id_state int4 NOT NULL
            );
              '''
    # Execute a command: this creates a new table
    cursor.execute(create_table_query)
    connection.commit()
    print("Table created successfully in PostgreSQL ")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


try:

    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="cbi2132015379"
    )
    cursor = connection.cursor()
    q = "insert into colony(colony,id_municipality,id_state)values(%s,%s,%s)"
    cursor.executemany(q, colony_list)
    connection.commit()
    print("Table updated successfully in PostgreSQL ")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
