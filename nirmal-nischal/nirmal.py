import cv2
data=r"C:\Users\ADMIN\Downloads\gow.jpeg"
img = cv2.imread(data)
if img is None:
    raise FileNotFoundError("Image not found. Please check the file path.")

# Resize for convenience (optional)
img = cv2.resize(img, (600, 400))

# Convert to grayscale and then to black & white
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
_, bw_img = cv2.threshold(gray, 127, 255, cv2.THRESH_BINARY)

# Create a named window
cv2.namedWindow("Toggle Image")

# State variable to track which image to show
show_bw = False

# Mouse click event handler
def toggle_image(event, x, y, flags, param):
    global show_bw
    if event == cv2.EVENT_LBUTTONDOWN:
        show_bw = not show_bw

# Set mouse callback
cv2.setMouseCallback("Toggle Image", toggle_image)

# Event loop
while True:
    if show_bw:
        cv2.imshow("Toggle Image", bw_img)
    else:
        cv2.imshow("Toggle Image", img)

    key = cv2.waitKey(1)
    if key == 27:  # ESC key to exit
        break

cv2.destroyAllWindows()