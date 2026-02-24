import os
import re

# Your input file path
data = r"D:\bigdata\drivers\10000Records.csv"
print(data)
# Extract the file name (without the path)
filename_with_extension = os.path.basename(data)
print(filename_with_extension)
# Remove the file extension
filename_without_extension = os.path.splitext(filename_with_extension)[0]
print(filename_without_extension)
# Remove special characters (replace hyphen with an empty string)
clean_filename = re.sub(r'[^a-zA-Z]', '', filename_without_extension)

print(clean_filename) 