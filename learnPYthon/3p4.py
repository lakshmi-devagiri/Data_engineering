'''3.4. Write a Python program to make a currency converter. The currency converter should be able to convert a specific currency to an appropriate INR (Indian National Rupee) value. (5 MARKS)
USD (U.S Dollars) (1 USD =71.83 INR)
YEN (Japanese YEN) (1 YEN= 0.66 INR
EURO (1 EURO=79.57 INR)
U.K. POUND (1 U.K POUND=93.11 INR)
INPUT AND OUTPUT SAMPLE:
Enter the currency which you want to convert. e.g. USD
Enter the value of the currency which you want to convert. e.g. 100
The total converted value is 7183.0'''
def convert_currency(currency, amount):
    exchange_rates = {"USD": 71.83, "YEN": 0.66, "EURO": 79.57, "POUND": 93.11}

    if currency.upper() in exchange_rates:
        conversion_rate = exchange_rates[currency.upper()]
        converted_value = amount * conversion_rate
        return converted_value
    else:
        return "Invalid currency code."

def main():
    currency = input("Enter the currency code (e.g., USD, YEN, EURO, POUND): ")
    amount = float(input("Enter the amount to convert: "))

    converted_value = convert_currency(currency, amount)

    if isinstance(converted_value, float):
        print(f"The total converted value is {converted_value:.2f} INR")
    else:
        print(converted_value)

if __name__ == "__main__":
    main()
