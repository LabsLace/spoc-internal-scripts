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
        insert_query = """ SELECT id, colony FROM colony_{}""".format(sheet)
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
    colony_df = pd.DataFrame(result, columns=["id", "colony"])

    # Connect excel and sheet file
# Connect excel and sheet file
    cp = pd.DataFrame()

    df_sheet = pd.read_excel('CPdescarga.xls', sheet)
    df_sheet = df_sheet[['d_codigo', 'd_asenta']]
    df_sheet = df_sheet.drop_duplicates()

    df_join = colony_df.join(df_sheet.set_index('d_asenta'), on='colony')
    cp = cp.append(df_join)
    cp = cp[['id', 'd_codigo']]
    postalCode_list = cp.values.tolist()

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
                CREATE TABLE public.postal_code_{} (
                    id serial4 NOT NULL,
                    postal_code int4 NOT NULL,
                    id_colony int4 NOT NULL
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
        q = "insert into postal_code_{}(id_colony,postal_code)values(%s,%s)".format(
            sheet)
        cursor.executemany(q, postalCode_list)
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

postal_code = pd.DataFrame()
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
        insert_query = """ SELECT postal_code,id_colony FROM postal_code_{}; """.format(
            sheet)
        cursor.execute(insert_query)
        result = cursor.fetchall()
        insert_query = """ SELECT id_state FROM municipality_{}; """.format(
            sheet)
        cursor.execute(insert_query)
        result2 = cursor.fetchall()
        #print(result)
        #print(result2)
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    state_df = pd.DataFrame(result2, columns=["id_state"])
    postal_code_df = pd.DataFrame(result, columns=["postal_code", "id_colony"])
    postal_code_df['id_state'] = state_df
    print(postal_code_df['id_state'][0])
    postal_code_df = postal_code_df[["postal_code", "id_colony", "id_state"]]
    postal_code_df = postal_code_df.fillna(postal_code_df['id_state'][0])
    postal_code = postal_code.append(postal_code_df)


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
            CREATE TABLE public.postal_code (
                id serial4 NOT NULL,
                postal_code int4 NOT NULL,
                id_colony int4 NOT NULL,
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


postal_code_list = postal_code.values.tolist()
try:

    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="cbi2132015379"
    )
    cursor = connection.cursor()
    q = "insert into postal_code(postal_code,id_colony,id_state)values(%s,%s,%s)"
    cursor.executemany(q, postal_code_list)
    connection.commit()
    print("Table updated successfully in PostgreSQL ")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
