import sys

# Convert arguments to floats and calculate sum
try:
    #all=[2,3,4,5,6,1,2,11]
    #numbers = [float(num) for num in all]
    numbers = [float(num) for num in sys.argv[1:]]
    total_sum = sum(numbers)
    print(f"entered numbers:", numbers)
    average = total_sum / len(numbers)
    print(f"Sum of numbers: {total_sum}")
    print(f"Average of numbers: {average}")
except ValueError:
    print("Please provide valid numbers.")