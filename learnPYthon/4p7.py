def capitalize(name):
    return name.capitalize()


usernames = ['rani', 'vani', 'Julia', 'Alice', 'Ela', 'ray', 'sunny', 'John', 'Filip', 'Jakub', 'Kuba', 'Robert',
             'Karol', 'Carol']


def main():
    print("Hi! My name is Bob and I am the security system robot.")

    while True:
        user_input = capitalize(input("What is your name?: "))

        if user_input in usernames:
            print(f"Hello {user_input}!")
            remove_option = input("Would you like to be removed from the system (yes/no)?: ").lower()

            if remove_option == 'yes':
                usernames.remove(user_input)
                print(usernames)
                print("No worries. You will not be on the list.")
        else:
            print(f"I think I have not seen you on the list {user_input}")
            add_option = input("Would you like to be added to the system (yes/no)?: ").lower()

            if add_option == 'yes':
                usernames.append(user_input)
                print(f"Hi! My name is Bob and I am the security system robot.")
                print(f"Hello {user_input}!")


if __name__ == "__main__":
    main()
