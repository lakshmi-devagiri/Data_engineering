"""let eg: running sports 4 members participate, ofcourse 3,2,5,6 or any number possible. Now i want to get that
"""
number_of_participants = int(input("How many people are participating in the running race? "))

#for loop temporary store data somewhere like this.
participants_times = []

# Get the user input for the participants' times
#let eg if u enter 6 , it 6 times u ll get enter popup box.
for i in range(number_of_participants):
  participant_time = int(input("Enter the time taken by the participants to complete the race in minutes: "))
  participants_times.append(participant_time)

def find_runner_up(participants_times):

  participants_times.sort()
  #get all elements in sort.. so starts from 0 so second person 1 so get like this.
  runner_up_time = participants_times[1]
  return runner_up_time

# Find the runner-up's time
runner_up_time = find_runner_up(participants_times)

# Print the runner-up's time
print("The runner up time is:", runner_up_time)
