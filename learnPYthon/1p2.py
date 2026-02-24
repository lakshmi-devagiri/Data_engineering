#Task : Write a Python program to build a numeric converter. The program will take integer from the user and convert it into binary, octal , hexadecimal, float , complex format.

#it means if u enter 4 i want binary and octal , hexa like that all format i want that number write functions like this.

def convert_to_binary(number):
    res = bin(number)[2:]
    return res
# bin() returns a string in the format '0b...', so [2:] removes the '0b'
#The slice [2:] is used to remove the first two characters ('0b') from the binary string

def convert_to_octal(number):
    octal_string = oct(number)[2:]
    return octal_string

def convert_to_hexadecimal(number):
    hexadecimal_string = hex(number)[2:]
    return hexadecimal_string

def convert_to_float(number):
  return float(number)

def convert_to_complex(number):
  return complex(number, 0)

# Get the number from the user.
number = int(input("Enter a number: "))
# Convert the number to different formats.
binary_number = convert_to_binary(number)
octal_number = convert_to_octal(number)
hexadecimal_number = convert_to_hexadecimal(number)
float_number = convert_to_float(number)
complex_number = convert_to_complex(number)
 # Print the results.
print(f"The binary of the number {number} is", binary_number)
print(f"The octal of the number  {number} is", octal_number)
print(f"The hexadecimal of the number  {number} is", hexadecimal_number)
print(f"The float of the numbers  {number} is", float_number)
print(f"The complex of the number {number} is", complex_number)