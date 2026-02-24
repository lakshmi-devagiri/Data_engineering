import sys
# perform_operation(add,4,3)
def perform_operation(operation, num1, num2):
    if operation == "add":
        return num1 + num2
    elif operation == "sub":
        return num1 - num2
    elif operation == "mul":
        return num1 * num2
    elif operation == "div":
        if num2 == 0:
            return "Error: Division by zero is not allowed."
        return num1 / num2
    else:
        return "Error: Unsupported operation."

def main():
    if len(sys.argv) != 4:
        print("Usage: python script_name.py <operation> <num1> <num2>")
        print("Supported operations: add, subtract, multiply, divide")
        return

    # Get arguments from command line
    operation = sys.argv[1].lower()
    try:
        num1 = float(sys.argv[2])
        num2 = float(sys.argv[3])
    except ValueError:
        print("Error: Both numbers must be valid numbers.")
        return

    # Perform the operation
    result = perform_operation(operation, num1, num2)
    print(f"Result: {result}")

if __name__ == "__main__":
    main()