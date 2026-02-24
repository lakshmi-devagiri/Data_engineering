import cv2
data=r"C:\Users\ADMIN\Downloads\gow.jpeg"
img = cv2.imread(data)
small_img = cv2.resize(img, (0, 0), fx=0.3, fy=0.3)  # fx and fy are scaling factors

cv2.imshow('Window Name', small_img)

cv2.waitKey(0)