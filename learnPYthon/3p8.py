def int_to_roman(integer):
  """Converts an integer to a Roman numeral.

  Args:
    integer: An integer to convert to a Roman numeral.

  Returns:
    A string representing the Roman numeral.
  """

  # Create a dictionary of Roman numerals and their corresponding integer values.
  roman_numeral_map = {
    1000: "M",
    500: "D",
    100: "C",
    50: "L",
    10: "X",
    5: "V",
    1: "I"
  }

  # Initialize the Roman numeral string.
  roman_numeral = ""

  # Iterate over the Roman numeral map in reverse order.
  for value, numeral in roman_numeral_map.items():
    # Divide the integer by the current value.
    quotient = integer // value

    # Add the current numeral to the Roman numeral string the quotient number of times.
    roman_numeral += numeral * quotient

    # Update the integer to be the remainder of the division.
    integer %= value

  # Return the Roman numeral string.
  return roman_numeral


# Example usage:
integer = int(input("Enter the number to convert to Roman Number "))

#integer = 417
roman_numeral = int_to_roman(integer)

print(f"The Roman numeral for the integer {integer} is: {roman_numeral}.")
