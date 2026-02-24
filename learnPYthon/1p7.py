#first try to understand Rock_paper_scissors what is this try to understand or google it.
#https://en.wikipedia.org/wiki/Rock_paper_scissors
'''
Write a program that lets the user play Rock-Paper-Scissors against the computer.¶
There should be five rounds,in every round user will get a chance to pick one item from [rock,paper,scissor] and computer will randomly select one.
Depending upon the selection either one of the player will get a point as per the rules or it will be draw if both the items are same.
In every round, player's choice and their current score should be updated and displayed.
After those five rounds, your program should print out who won and lost or that there is a tie.
'''
import random
# Define the possible choices for the user and computer
choices = ["rock", "paper", "scissors"]
# Initialize the user and computer scores
user_score = 0
computer_score = 0

# Start the game loop
for i in range(5):
  # Get the user's choice
  print("Please enter either rock or paper, or scissors")
  user_choice = input("Enter your choice  ")

  # Check if the user's choice is valid
  if user_choice not in choices:
    print("Invalid choice. Please enter either rock or paper, or scissors.")
    continue

  # Generate the computer's choice randomly
  computer_choice = random.choice(choices)

  # Determine the winner of the round
  if user_choice == "rock" and computer_choice == "scissors":
    user_score += 1
  elif user_choice == "scissors" and computer_choice == "paper":
    user_score += 1
  elif user_choice == "paper" and computer_choice == "rock":
    user_score += 1
  elif user_choice == computer_choice:
    # It's a tie
    pass
  else:
    computer_score += 1

  # Display the results of the round
  print("---------------------Round {}----------------------------".format(i + 1))
  print("Your choice:", user_choice)
  print("Computer's choice:", computer_choice)
  print("Scoreboard: Your score {} , computer's score {}".format(user_score, computer_score))

# Determine the overall winner of the game
if user_score > computer_score:
  print("Congratulations!!! You won!!!")
elif user_score < computer_score:
  print("Sorry, you lost. Better luck next time!")
else:
  print("It's a tie!")