#2.1
items_in_store = {"soap": 5, "brush": 10, "paste": 7, "chips": 10}
item_price = {"soap": 100, "brush": 30, "paste": 50, "chips": 10}
shopping_list = {}

while True:
    # Get the item name from the user.
    item_name = input("Enter the item name: ")

    # Check if the item is available in the store.
    if item_name in items_in_store:
        # Get the quantity of the item from the user.
        quantity = int(input("Enter the quantity: "))

        # Check if the quantity is available in the store.
        if quantity <= items_in_store[item_name]:
            # Add the item and quantity to the shopping list.
            shopping_list[item_name] = quantity
            # Update the quantity of the item in the store.
            items_in_store[item_name] -= quantity
        else:
            print("Out of stock.")
    else:
        print("Item not available in the store.")

    # Ask the user if they want to continue shopping.
    continue_shopping = input("Do you want to continue shopping? (y/n) ").lower()
    if continue_shopping != "y":
        break

# Print the shopping list with prices.
print("Shopping list:")
total_cost = 0

for item_name, quantity in shopping_list.items():
    price = item_price.get(item_name, 0) # item_price.get(item_name) it return None by default,
    cost = quantity * price
    print(f"{item_name}: {quantity} * {price} = {cost}")
    total_cost += cost

print("Total Cost:", total_cost)
