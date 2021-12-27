import psycopg2
from psycopg2 import Error
import unidecode
import pandas as pd
import xlrd
import os


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
    insert_query = """ SELECT id, state FROM state"""
    cursor.execute(insert_query)
    result = cursor.fetchall()
    print(result)


except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

df = pd.DataFrame(result, columns=["id_state", "state"])


xls = xlrd.open_workbook(r'CPdescarga.xls', on_demand=True)
sheet_names = xls.sheet_names()
del sheet_names[0]
sheet_names

# Connect excel and sheet file
for sheet in sheet_names:
    municipality_df = pd.DataFrame()

    df_sheet = pd.read_excel('CPdescarga.xls', sheet)
    df_sheet = df_sheet[['D_mnpio']]
    df_sheet = df_sheet.drop_duplicates()

    df_sheet['state'] = unidecode.unidecode(sheet)
    df_sheet['state'] = df_sheet['state'].str.replace('_', ' ').replace('Distrito Federal', 'Ciudad de México').replace('Coahuila de Zaragoza', 'Coahuila').replace(
        'San Luis Potosi', 'San Luis Potosí').replace('Coahuila de Zaragoza', 'Coahuila').replace('Queretaro', 'Querétaro')
    df_sheet['state'] = df_sheet['state'].str.replace('Michoacan de Ocampo', 'Michoacán').replace('Veracruz de Ignacio de la Llave', 'Veracruz').replace(
        'Mexico', 'Estado de México').replace('Nuevo Leon', 'Nuevo León').replace('Yucatan', 'Yucatán')

    municipality_df = municipality_df.append(df_sheet)
    test = municipality_df.join(df.set_index('state'), on='state')
    municipality = test[['D_mnpio', 'id_state']]
    municipality_list = municipality.values.tolist()

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
                CREATE TABLE public.municipality_{} (
                    id serial4 NOT NULL,
                    municipality varchar(50) NOT NULL,
                    id_state int4 NOT NULL
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
        q = "insert into municipality_{} (municipality,id_state)values(%s,%s)" .format(
            sheet)
        cursor.executemany(q, municipality_list)
        connection.commit()
        print("Table updated successfully in PostgreSQL ")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    #print(sheet)


""" Merge databases """

municipality = pd.DataFrame()
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
        insert_query = """ SELECT municipality,id_state FROM municipality_{}; """.format(
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
    municipality_df = pd.DataFrame(
        result, columns=["municipality", "id_state"])

    print(sheet)
    municipality = municipality.append(municipality_df)

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
                municipality varchar(50) NOT NULL,
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


municipality_list = municipality.values.tolist()
try:

    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="cbi2132015379"
    )
    cursor = connection.cursor()
    q = "insert into municipality(municipality,id_state)values(%s,%s)"
    cursor.executemany(q, municipality_list)
    connection.commit()
    print("Table updated successfully in PostgreSQL ")

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")


