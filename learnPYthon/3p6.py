'''3.6.generated multiplication questions to do. After each, the program should tell them whether they got it right or wrong and what the correct answer is and finally it should display the total score out of 10 scored by a user.(10 marks) For Example:
#my requirement i want to ask 10 questions like below.. if u manually enter right answer ok fine if u entered wrong answers u must return Wrong and show correct answer like this.

Question 1: 3 x 4 = 12 Right!
Question 2: 8 x 6 = 44 Wrong. The answer is 48.
...in this way display 10 times finally get how many times u entered right answers show that.
Question 10: 7 x 7 = 49
Right.'''
import random
import random

def generate_multiplication_question():
    num1 = random.randint(1, 10)
    num2 = random.randint(1, 10)
    return f"{num1} x {num2}", num1 * num2

def ask_question(question):
    user_answer = int(input(question + " = "))
    return user_answer

def main():
    score = 0

    for i in range(1, 11):
        question, correct_answer = generate_multiplication_question()
        user_answer = ask_question(f"Question {i}: {question}")

        if user_answer == correct_answer:
            print("Right!")
            score += 1
        else:
            print(f"Wrong. The correct answer is {correct_answer}.")

    print(f"Total Score: {score}/10")

if __name__ == "__main__":
    main()
