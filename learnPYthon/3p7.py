#Write a Python Program to create the following series and then print the nth Prime number from the Series,0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89..
#ur requirement is get prime numbers

def is_prime(num):
    """Check if a number is prime."""
    if num < 2:
        return False
    for i in range(2, int(num**0.5) + 1):
        if num % i == 0:
            return False
    return True

def find_nth_prime_in_series(fibonacci_series, n):
    """Find the nth prime number within the Fibonacci series."""
    primes = []
    for num in fibonacci_series:
        if is_prime(num):
            primes.append(num)
        if len(primes) == n:
            return primes[-1]
    return None

def fibonacci_series(n):
    """Generate Fibonacci series up to n terms."""
    series = [0, 1]
    while len(series) < n:
        series.append(series[-1] + series[-2])
    return series



# Get user input for the number of terms and the position of the prime number
n_terms = int(input("Enter the number of terms for the Fibonacci series (min 24): "))
position_of_prime = int(input("Enter the position of the prime number to find: "))

# Generate the Fibonacci series and find the nth prime number
fibonacci_result = fibonacci_series(n_terms)
nth_prime_in_fibonacci = find_nth_prime_in_series(fibonacci_result, position_of_prime)

# Output the result
print(f"Fibonacci series up to {n_terms} terms:")
print(fibonacci_result)
print(f"\nThe {position_of_prime}th prime number within the Fibonacci series is: {nth_prime_in_fibonacci}")
