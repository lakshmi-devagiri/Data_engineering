#Suppose the cover price of the book ‘Steve Jobs: The Life, Lessons & Rules for Success’ is 49.65, Harvard bookstores get a 35 percent discount from the publisher " Createspace Independent Pub”. Shipping costs are 4.65 for the first copy and 78 cents for each additional copy from the publisher. What is the total wholesale cost for 60, 100 and 150 copies?
def calculate_total_wholesale_cost(cover_price, discount, number_of_books):
#Calculates the total wholesale cost and money saved for a given cover price, discount, and number of books.

  discount_price = cover_price * (1 - discount)
  shipping_cost = calculate_shipping_cost(number_of_books)
  total_wholesale_cost = discount_price * number_of_books + shipping_cost
  money_saved = cover_price * number_of_books - total_wholesale_cost
  return total_wholesale_cost, money_saved

def calculate_shipping_cost(number_of_books):
  shipping_cost = 4.65 + (number_of_books - 1) * 0.78
  return shipping_cost

cover_price = 49.65
discount = 0.35

number_of_books_60 = int(input("Enter the number of books bought for 60 copies: "))
number_of_books_100 = int(input("Enter the number of books bought for 100 copies: "))
number_of_books_150 = int(input("Enter the number of books bought for 150 copies: "))

# Calculate the total wholesale cost and money saved for 60, 100, and 150 copies
total_wholesale_cost_60, money_saved_60 = calculate_total_wholesale_cost(cover_price, discount, number_of_books_60)
total_wholesale_cost_100, money_saved_100 = calculate_total_wholesale_cost(cover_price, discount, number_of_books_100)
total_wholesale_cost_150, money_saved_150 = calculate_total_wholesale_cost(cover_price, discount, number_of_books_150)

# Display the cost of a number of books bought, wholesale costs, and money saved
print("The cost of 60 books bought is:", total_wholesale_cost_60, "and you saved:", money_saved_60)
print("The cost of 100 books bought is:", total_wholesale_cost_100, "and you saved:", money_saved_100)
print("The cost of 150 books bought is:", total_wholesale_cost_150, "and you saved:", money_saved_150)
