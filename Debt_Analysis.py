from Database_Operations import get_debt_by_country

def main():
    db_name = 'International_Debt_Data'
    user = 'postgres'
    password = 'admin123'
    host = 'localhost'
    port = '5432'

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
