'''3.3 A famous hotel is maintaining its order history in the dictionary in the given format. Find out which food item is ordered the most (5 Marks)¶
order_id: [list of items ordered]

order_history= {165:['Noodles','Tomato Soup','Fries'],168:['Rice Idli','Rawa Dosa','Fries'],190:['Fries','Noodles']}'''
order_history = {
    165: ['Noodles', 'Tomato Soup', 'Fries'],
    168: ['Rice Idli', 'Rawa Dosa','Fries', 'Noodles'],
    190: ['Fries', 'Noodles']
}

# Flatten the list of items from order_history
all_items = [item for items in order_history.values() for item in items]

# Count the occurrences of each item
item_counts = {item: all_items.count(item) for item in set(all_items)}

# Find the most ordered item
most_ordered_item = max(item_counts, key=item_counts.get)

print(f"The most ordered food item is: {most_ordered_item}")
