def calculate_difference_in_readings(patient_data, healthy_patient_data):
#Calculates the difference in readings between the patient's input data and the healthy patient's data.
  differences = {}
  for component in patient_data:
    differences[component] = patient_data[component] - healthy_patient_data[component]

  return differences


# Calculate the difference in readings.
differences = calculate_difference_in_readings(patient_data, healthy_patient_data)

# Print the difference in readings.
for component in differences:
  print(f"The difference in your {component} is {differences[component]}.")
