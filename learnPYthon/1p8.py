# Ask the user to enter 10 test scores. Write five different functions to do the following:
#let eg: u have worldcup cricket, each team plays 10 matches , how much u got highest score like that diff thing i want to get.
#like highest and lowest scores get that from 10 games..
def highest_and_lowest_scores(scores):
    highest_score = max(scores)
    lowest_score = min(scores)
    print("The highest score is:", highest_score)
    print("The lowest score is:", lowest_score)

def average_score(scores):
    total_score = sum(scores)
    number_of_scores = len(scores)
#to get avg score formula
    average_score = total_score / number_of_scores
    print("The average score is:", average_score)

def second_largest_score(scores):
    """Prints the second largest score in a list of scores."""
    scores.sort(reverse=True)
    second_largest_score = scores[1]
#same logic u ll get in another program (1p5) progogram so understand.
    print("The second largest score is:", second_largest_score)

def check_for_scores_over_100(scores):
    """Checks if any of the scores in a list of scores is greater than 100. If so, prints a warning message to the user."""

    for score in scores:
        if score > 100:
            print("WARNING: A value over 100 has been entered.")
            return


def drop_two_lowest_scores(scores):
    """Drops the two lowest scores in a list of scores and prints out the average of the rest of them."""

    scores.sort()
    scores.pop(0)
    scores.pop(0)
    #get score above 100 scores only

    #if u sort u ll get top hight to lowest .. pop remove by default from lowest...so pop call two times to remove last 2

    average_score = sum(scores) / len(scores)

    print("The average score after dropping the two lowest scores is:", average_score)

# Get the user to enter 10 test scores
scores = []
for i in range(10):
    score = int(input("Enter your score: "))
    scores.append(score)

# Check for scores over 100
check_for_scores_over_100(scores)

# Print the highest and lowest scores
highest_and_lowest_scores(scores)

# Print the average of the scores
average_score(scores)

# Print the second largest score
second_largest_score(scores)

# Drop the two lowest scores and print out the average of the rest of them
drop_two_lowest_scores(scores)
