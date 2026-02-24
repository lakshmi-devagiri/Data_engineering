#Write a Python program to check the validity of a debit card.
import re
def is_debit_card_valid(debit_card_number, expiry_year, expiry_month, cvv_number):
  # few members entered credit card with spaces remove those spaces use regular expression package
  #more info check https://docs.python.org/3/library/re.html
  debit_card_number = re.sub(r"\s+", "", debit_card_number)
## task a) Verify and ensure that the length of the debit card is 14.( 2marks)

  if len(debit_card_number) != 14:
    return False

  #Task b) Verify that the year printed on the debit card should be either 2021 or greater than 2021.
  # If the year is less than 2021, user should get a message that the card expired.(3 marks)

  if expiry_year < 2021:
    return False

  ##c)Verify that the month number printed in the card should be between 1 and 12.
  # Any number less than 1 and any number greater than 12 should be invalid.(3 marks)
  if expiry_month < 1 or expiry_month > 12:
    return False

##d) Enter the CVV number printed on the debit card and ensure that the length is neither less than 3 nor greater than 3.(2 marks)

  # Check if the CVV number length is 3.
  if len(cvv_number) != 3:
    return False

  # If all checks pass, the debit card is valid.
  return True


# Get the user input for the debit card number, expiry year, expiry month, and CVV number.
debit_card_number = input("Enter the debit card number: ")
expiry_year = int(input("Enter the expiry year: "))
expiry_month = int(input("Enter the expiry month: "))
cvv_number = input("Enter the CVV number: ")

# Check if the debit card is valid.
is_debit_card_valid = is_debit_card_valid(debit_card_number, expiry_year, expiry_month, cvv_number)


# Display the result to the user.
if is_debit_card_valid:
  print("The debit card is valid.")
else:
  print("The debit card is not valid.")

'''
Testing purpose enter these inputs
Enter the debit card number: 23451234123412
Enter the expiry year: 2022
Enter the expiry month: 12
Enter the CVV number: 234

U ll get results like this
The debit card is valid.
'''