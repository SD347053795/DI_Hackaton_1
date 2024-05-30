from Database_Operations import create_database, create_table, upload_csv_to_postgresql

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
    print("Data upload completed successfully.")

if __name__ == "__main__":
    main()
