'''
3.1. Ram built his own house and was confused about what name he should keep for it. He asked his friend Shyam to help. Shyam suggested a lot of names. Ram has certain conditions for the name: (5 marks)
• It should consist of exactly three distinct characters, say C1, C2 and C3 • It should satisfy the criteria that the string was of the form - C1n C2n C3n: This means, first C1 occurs n times, then C2 occurs n times and then C3 occurs n times. For example, xyz, ccaarr, mmmiiiaaa satisfy the criteria, but xyzw, aabbbcccc don't. Given N names suggested by Shyam, print "OK" if Ram likes the name and "Not OK" if he doesn't.

• Input: First-line contains a single positive integer N - the number of names. N lines follow - each line contains a name. o
Output: For each name, Print "OK" if the name satisfies the criteria, else print "Not OK", on a new line. Constraints: 1 ≤ N ≤ 100 1 ≤ Length of names ≤ 500 Names contain only lowercase English alphabets

SAMPLE INPUT 2

bbbrrriii

brian
'''
def is_valid_name(name):
    count = len(name) // 3
    return len(name) % 3 == 0 and all(name.count(name[i]) == count for i in range(3))

# Input: Number of names
N = int(input("How many words u want to test?"))

# Process each name
for _ in range(N):
    name = input("enter name:").strip()

    # Check if the name satisfies Ram's conditions
    if is_valid_name(name):
        print("OK")
    else:
        print("Not OK")
