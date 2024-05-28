import csv
import psycopg2


def database_exists(db_name, user, password, host, port):
    conn = psycopg2.connect(dbname='International_Debt_Data', user= 'postgres', password= 'admin123', host='localhost', port= '5432')
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cursor.fetchone()
    cursor.close()
    conn.close()
    return exists is not None


def create_database(db_name, user, password, host, port):
    conn = psycopg2.connect(dbname='International_Debt_Data', user= 'postgres', password= 'admin123', host='localhost', port= '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    if not database_exists(db_name, user, password, host, port):
        create_db_query = f"CREATE DATABASE {db_name};"
        cursor.execute(create_db_query)
    cursor.close()
    conn.close()


def create_table(db_name, user, password, host, port):
    conn = psycopg2.connect(dbname='International_Debt_Data', user= 'postgres', password= 'admin123', host='localhost', port= '5432')
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS International_Debt_Statistics (
        Country_Name VARCHAR(100),
        Debt_Per_Capita NUMERIC,
        Total_External_Debt NUMERIC,
        Percentage_of_GDP NUMERIC,
        Percentage_of_Total_Wealth NUMERIC,
        Year INT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()


def upload_csv_to_postgresql(csv_file_path, db_name, user, password, host, port):
    conn = psycopg2.connect(dbname='International_Debt_Data', user= 'postgres', password= 'admin123', host='localhost', port= '5432')
    cursor = conn.cursor()

    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            insert_query = """
                INSERT INTO International_Debt_Statistics (Country_Name, Debt_Per_Capita, Total_External_Debt, Percentage_of_GDP, Percentage_of_Total_Wealth, Year) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                row['Country_Name'],
                row['Debt_Per_Capita'].replace(',', '') if row['Debt_Per_Capita'] else None,
                row['Total_External_Debt'].replace(',', '') if row['Total_External_Debt'] else None,
                row['Percentage_of_GDP'] if row['Percentage_of_GDP'] else None,
                row['Percentage_of_Total_Wealth'] if row['Percentage_of_Total_Wealth'] else None,
                row['Year'] if row['Year'] else None
            ))

    conn.commit()
    cursor.close()
    conn.close()


def get_debt_by_country(country_name, db_name, user, password, host, port):
    conn = psycopg2.connect(dbname='International_Debt_Data', user= 'postgres', password= 'admin123', host='localhost', port= '5432')
    cursor = conn.cursor()
    query = "SELECT Total_External_Debt, Year FROM International_Debt_Statistics WHERE Country_Name = %s"
    cursor.execute(query, (country_name,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result


def main():
    db_name = 'International_Debt_Data'
    user = 'postgres'
    password = 'admin123'
    host = 'localhost'
    port = '5432'

    create_database(db_name, user, password, host, port)
    create_table(db_name, user, password, host, port)
    upload_csv_to_postgresql(r"C:\Users\Shmuel\Desktop\DI_Hackatons\DI_Hackaton_1\World_Debt_Statistics.csv", db_name,
                             user, password, host, port)

    while True:
        country = input("Enter the country name (or enter 1 to exit): ")
        if country == '1':
            print("Exiting the program.")
            break

        result = get_debt_by_country(country, db_name, user, password, host, port)
        if result:
            total_debt, year = result
            print(f"Total External Debt for {country}: {total_debt}, Year: {year}")
        else:
            print("Sorry, that country does not exist, please check your spelling.")


if __name__ == "__main__":
    main()
