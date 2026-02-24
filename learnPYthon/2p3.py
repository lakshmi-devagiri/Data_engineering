#2.3 Python program that accepts a comma separated sequence of words as input and after sorting them alphabetically.
input_sequence = input("Enter a comma-separated sequence of words: ")
words = input_sequence.split(',')
sorted_words = sorted(words)

# Join the sorted words into a string
output_sequence = ','.join(sorted_words)
print("OUTPUT:", output_sequence)
