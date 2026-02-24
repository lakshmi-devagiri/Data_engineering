import turtle

# Set up the screen
screen = turtle.Screen()
screen.bgcolor("skyblue")

# Create turtle
pen = turtle.Turtle()
pen.speed(3)
pen.pensize(2)

# Function to draw a rectangle
def draw_rectangle(t, width, height, color):
    t.begin_fill()
    t.fillcolor(color)
    for _ in range(2):
        t.forward(width)
        t.left(90)
        t.forward(height)
        t.left(90)
    t.end_fill()

# Function to draw a semicircle (for domes)
def draw_dome(t, radius, color):
    t.begin_fill()
    t.fillcolor(color)
    t.circle(radius, 180)
    t.end_fill()

# Draw base platform
pen.penup()
pen.goto(-150, -100)
pen.setheading(0)
pen.pendown()
draw_rectangle(pen, 300, 100, "white")

# Draw main central building
pen.penup()
pen.goto(-100, 0)
pen.setheading(0)
pen.pendown()
draw_rectangle(pen, 200, 100, "white")

# Draw main dome
pen.penup()
pen.goto(0, 100)
pen.setheading(180)
pen.pendown()
draw_dome(pen, 50, "white")

# Function to draw a minaret with a small dome
def draw_minaret(x, y):
    pen.penup()
    pen.goto(x, y)
    pen.setheading(0)
    pen.pendown()
    draw_rectangle(pen, 20, 150, "white")
    pen.penup()
    pen.goto(x + 10, y + 150)
    pen.setheading(180)
    pen.pendown()
    draw_dome(pen, 10, "white")

# Left minaret
draw_minaret(-180, -100)

# Right minaret
draw_minaret(160, -100)

# Draw door on the central building
pen.penup()
pen.goto(-20, 0)
pen.setheading(0)
pen.pendown()
draw_rectangle(pen, 40, 60, "lightgray")

# Hide the turtle and keep the window open
pen.hideturtle()
screen.mainloop()
