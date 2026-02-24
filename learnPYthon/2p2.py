#2.2 There are Government rules for a particular age group to watch specific movies. Some movies are advisable for students and some are not advisable for a specific age group. Nowadays, people have the habit of watching movies online. Before watching the movies find the age limit. If the age limits wrong, you are not providing the tickets
# a) Create a dictionary of age limit according to movie names
movie_limits = {
    "A Little Princess": {"age_limit": 11, "seats": 5},
    "Millennium": {"age_limit": 18, "seats": 5},
    "Hangover": {"age_limit": 15, "seats": 7},
    "Frozen": {"age_limit": 12, "seats": 4},
    "Hangover II": {"age_limit": 15, "seats": 6},
    "Shrek": {"age_limit": 12, "seats": 5},
}

# b) Ask the user to choose a movie
print("Available Movies:")
for movie in movie_limits:
    print(movie)

chosen_movie = input("What film would you like to watch?: ")

# c) Ask the user for their age
user_age = int(input("How old are you?: "))

# d) Check age limit and ticket availability
if chosen_movie in movie_limits:
    age_limit = movie_limits[chosen_movie]["age_limit"]
    available_seats = movie_limits[chosen_movie]["seats"]

    if user_age >= age_limit and available_seats > 0:
        print("Enjoy the movie!")
        movie_limits[chosen_movie]["seats"] -= 1
    elif user_age < age_limit:
        print("You are too young to watch this movie.")
    else:
        print("Sorry, no tickets available.")
else:
    print("Sorry, the selected movie is not available.")
