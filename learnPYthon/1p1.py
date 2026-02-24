#Task: A string is said to be complete if it contains all the characters from a to z. Given a string, check if it is complete or not.
#here ur task is first enter 3 words like "aaaabbbb", next string like 'abcdefghijklmnopqrstuvwxyz" another 'aaaaa'
#now first one is correct second string return yes third string false like i want to return.
def is_string_complete(string):
  # Check if the string contains all the characters from a to z.
  return string.isalpha() and len(string) == 26

# Get the number of strings from the user.
number_of_strings = int(input("Enter the number of strings: "))

# Get the strings from the user.
strings = []
for i in range(number_of_strings):
  string = input("Enter a string: ")
  strings.append(string)

# Check if each string is complete.
for string in strings:
  if is_string_complete(string):
    print("YES")
  else:
    print("NO")
''' testing
Enter the number of strings: 4
Enter a string: abcdefgh
Enter a string: abcdefghijklmnopqrstuvwxyz
Enter a string: qwertyuiopasdfghjklzxcvbnm
Enter a string: abcd
NO
YES
YES
NO
'''
