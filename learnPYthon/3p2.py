'''3.2. You are working with NASA and suddenly you got an opportunity to travel to the moon. You have to stay there for 15 years.
Exercise1: You have to calculate your weight in kilograms on the moon in a period of 15 years,(3 marks)

Step1: Measure your Weight on the Moon and also create a variable to store it. Assume your starting weight:30
● For each year, you can calculate the new weight by adding a kilogram, and then multiplying by 6.5 percent (0.65) to get the weight on the moon:
Exercise2: Create basic Moon Weight Function: The function should take two parameters: current weight:30 and increased weight:0.5 (the amount the weight will increase each year) (2 marks)

Note: moon_weight = weight * 0.65'''
def calculate_moon_weight(weight, increase_per_year):
  """Calculates the weight on the moon for a given weight and increase per year.

  Args:
    weight: The weight in kilograms.
    increase_per_year: The amount the weight will increase each year.

  Returns:
    The weight on the moon in kilograms.
  """

  moon_gravity = 0.17
  weight_on_moon = weight * moon_gravity

  for year in range(15):
    weight_on_moon += increase_per_year
    weight_on_moon *= 1.065

  return weight_on_moon

# Example usage:

weight = 30
increase_per_year = 0.5

weight_on_moon = calculate_moon_weight(weight, increase_per_year)

print(f"Your weight on the moon in 15 years will be {weight_on_moon:.2f} kilograms.")

