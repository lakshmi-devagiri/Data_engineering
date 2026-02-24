import cv2
import numpy as np
import torch
from realesrgan import *

# Set the path where you saved the model weights
model_path = r"C:\Users\ADMIN\Pictures\Real-ESRGAN-master\weights\RealESRGAN_x4.pth"

# Load the Real-ESRGAN model
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model = RealESRGAN(device, scale=4)
model.load_weights(model_path)  # Load the pre-trained model weights

# Load the low-resolution image
image_path = r"C:\Users\ADMIN\Downloads\myphotoFrame.jpeg"  # Change this to your image file path
lr_image = cv2.imread(image_path)
lr_image = cv2.cvtColor(lr_image, cv2.COLOR_BGR2RGB)  # Convert to RGB format

# Apply AI Upscaling (First pass: 4X)
sr_image = model.predict(lr_image)

# Convert back to OpenCV format
sr_image = cv2.cvtColor(sr_image, cv2.COLOR_RGB2BGR)

# Second Pass: Upscale 4X Again (To achieve 16X resolution)
sr_image_16x = model.predict(sr_image)

# Save final high-resolution image
output_path = r"C:\Users\ADMIN\Pictures\high_res_16x.jpeg"
cv2.imwrite(output_path, sr_image_16x)

print(f"High-resolution image saved at {output_path}")
