from rembg import remove
from PIL import Image
input=r"C:\Users\ADMIN\Downloads\venu_flight.jpeg"
output=r"C:\Users\ADMIN\Downloads\venu_flight_rmp.png"
ip = Image.open(input)
'''op = remove(ip)
op.save(output)'''
# Open the input image
with Image.open(input) as ip:
    # Remove the background
    op = remove(ip)

    # Convert to RGB mode if the image is in RGBA
    if op.mode == "RGBA":
        op = op.convert("RGB")

    # Save the image as JPEG
    op.save(output, format="JPEG")
    #op.save(output_path, format="PNG")

#pip install --upgrade charset_normalizer rembg Pillow
''' Read multiple files from a folder
import os 
os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.lower().endswith((".jpg", ".jpeg", ".png")):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, filename)

        with Image.open(input_path) as ip:
            op = remove(ip)
            if op.mode == "RGBA":
                op = op.convert("RGB")
            op.save(output_path, format="JPEG")'''

'''
3. Resize Image After Background Removal
new_size = (800, 800)  # Width, Height

with Image.open(input_path) as ip:
    op = remove(ip)
    if op.mode == "RGBA":
        op = op.convert("RGB")
    op = op.resize(new_size)
    op.save(output_path, format="JPEG")
'''
''' Apply Watermark After Removing Background
from PIL import ImageDraw, ImageFont

watermark_text = "Sreyobhilashi IT +91-9247159150"
with Image.open(input_path) as ip:
    op = remove(ip)
    if op.mode == "RGBA":
        op = op.convert("RGB")
    
    draw = ImageDraw.Draw(op)
    font = ImageFont.truetype("arial.ttf", 36)  # Ensure the font is installed
    text_size = draw.textsize(watermark_text, font=font)
    position = (op.width - text_size[0] - 10, op.height - text_size[1] - 10)
    draw.text(position, watermark_text, font=font, fill="white")
    
    op.save(output_path, format="JPEG")
'''

