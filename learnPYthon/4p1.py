def search_inventory(inventory, item):
    for category, items in inventory.items():
        if item in items:
            return f"{item} is available in the {category} category."

    return None

def main():
    inventory = {
        'Men': ['Shirt', 'Suit', 'Trouser', 'Jeans'],
        'Women': ['Saree', 'Dress_suit', 'Handbag', 'Jewellery'],
        'Home_appliances': ['TV', 'Refrigerator'],
        'Kids': ['Toys', 'Bags', 'Books']
    }

    while True:
        item_to_buy = input("Enter the item you want to buy: ")
        result = search_inventory(inventory, item_to_buy)

        if result:
            print(result)
            break
        else:
            continue_buying = input("\n Item not found. Would you like to continue buying? (yes/no): ")
            if continue_buying.lower() != "yes":
                break

if __name__ == "__main__":
    main()


'''An Ecommerce portal has a list of items available with them in each category (inventory) in the form of the list within the Dictionary.
(10 marks)

For example Inventory= {'Men': ['Shirt','Suit','Trouser','Jeans'],

'Women': ['Saree','Dress_suit','Handbag','Jewellery'],

'Home_appliances': ['TV','Refrigerator'],

'Kids':['Toys','Bags','Books']}

Write a program which will take the name of an item, the user wants to buy and search it in the inventory. if it’s found in the inventory then print a message like 'item is available'. otherwise ask the user 'Would you like to continue buying: yes or no'. If the user gives 'yes ' then repeat searching in the inventory otherwise program should stop the execution.'''