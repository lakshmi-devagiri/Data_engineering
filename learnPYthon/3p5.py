'''3.5. A Basket of Halloween candy has an unknown amount of candy and you need to guess exactly how much candy is in the bowl. You ask the person in charge a few questions to make a correct guess. If the candy is divided evenly among 5 people, how many pieces would be leftover? The answer is 2 pieces. You then ask about dividing the candy evenly among 6 people, and the amount left over is 3 pieces. Finally, you ask about dividing the candy evenly among 7 people, and the amount left over is 2 pieces. By looking at the bowl, you can tell that there are less than 200 pieces. Write a program to determine how many pieces are in the bowl. (10 Marks)
SAMPLE INPUT: If the candy is divided evenly among 5 people, how many pieces would be leftover? 2
If the candy is divided evenly among 6 people, how many pieces would be leftover? 3
If the candy is divided evenly among 7 people, how many pieces would be leftover? 2

SAMPLE OUTPUT: Number of candies in the jar: 177
SAMPLE INPUT : If the candy is divided evenly among 5 people, how many pieces would be leftover? 2
If the candy is divided evenly among 6 people, how many pieces would be leftover? 4
If the candy is divided evenly among 7 people, how many pieces would be leftover? 2
SAMPLE OUTPUT : Number of candies in the jar: 142'''
def find_number_of_candies():
    for candies in range(1, 200):
        if candies % 5 == 2 and candies % 6 == 3 and candies % 7 == 2:
            return candies

result = find_number_of_candies()
print(f"Number of candies in the jar: {result}")
