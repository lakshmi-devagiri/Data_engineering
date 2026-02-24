
input="VenuFROM@4319"
#task: lower char must get first in asc order next get upper characters next get integers finally special characters

low="".join( c for c in input if c.islower())
upp = "".join(c for c in input if c.isupper())
num = "".join(c for c in input if c.isdigit())
spec="".join(c for c in input if not c.isalnum())

#res=low + upp+num + spec
print(low + upp + num + spec)