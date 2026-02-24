'''4.2. Write a Python program which calculates the area of various geometrical figures as given below: (5 marks)
The program should ask the user about the geometrical figure for which he wants to calculate the area. Take the inputs accordingly and calculate the area. (units/metrics like mm, cm can be included as per the programmer’s choice)

the formula to calculate the area of a square is Area= (Side of a square)**2
The formula to calculate the area of a circle is Area=(pi)*(radius)**2
The formula to calculate the area of a triangle is Area=0.5Base Height
The formula to calculate area of a cylinder is Area=2piradiusheight+2pi*(radius**2)
SAMPLE INPUT AND OUTPUT:
Please enter the name of the geometrical figure for which you want to calculate the area CYLINDER
Enter the radius of a cylinder = 3
Enter the height of a cylinder = 5
The area of the cylinder 150.72'''
import math

def calculate_area_of_square(side):
  """Calculates the area of a square.

  Args:
    side: The length of a side of the square.

  Returns:
    The area of the square.
  """

  return side * side

def calculate_area_of_circle(radius):
  """Calculates the area of a circle.

  Args:
    radius: The radius of the circle.

  Returns:
    The area of the circle.
  """

  return math.pi * radius * radius

def calculate_area_of_triangle(base, height):
  """Calculates the area of a triangle.

  Args:
    base: The base of the triangle.
    height: The height of the triangle.

  Returns:
    The area of the triangle.
  """

  return 0.5 * base * height

def calculate_area_of_cylinder(radius, height):
  """Calculates the area of a cylinder.

  Args:
    radius: The radius of the cylinder.
    height: The height of the cylinder.

  Returns:
    The area of the cylinder.
  """

  return 2 * math.pi * radius * height + 2 * math.pi * radius ** 2

def main():
  """Calculates the area of various geometrical figures."""

  # Get the user input
  geometrical_figure = input("Please enter the name of the geometrical figure for which you want to calculate the area: ").upper()
  radius = float(input("Enter the radius: "))
  height = float(input("Enter the height: "))

  # Calculate the area
  if geometrical_figure == "SQUARE":
    area = calculate_area_of_square(radius)
  elif geometrical_figure == "CIRCLE":
    area = calculate_area_of_circle(radius)
  elif geometrical_figure == "TRIANGLE":
    area = calculate_area_of_triangle(radius, height)
  elif geometrical_figure == "CYLINDER":
    area = calculate_area_of_cylinder(radius, height)
  else:
    print("Invalid geometrical figure.")
    return

  # Display the result
  print(f"The area of the {geometrical_figure} is {area}.")

if __name__ == "__main__":
  main()
