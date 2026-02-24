lst = ['a', 2,4,5,9,1,'b',4,3,2,9,5,4,3,'c',9,2,1,8]
#this value is in Key, value.
#i want to consider a as key, remaining numbers like value.
#i want {'a': [2,4,5,9,1],
         #'b':[4,3,2,9,5,4,3], 'c':[9,2,1,8]}

#i want get untill get new string get all values like list

# to get expected results data must be in dict
#thats y use dict as temp element
#step 1)
temp = {}
curr_key=None
curr_value = []
#step 2
for x in lst:
    if isinstance(x,str):
        if curr_key is not None:
            temp[curr_key]= curr_value
        curr_key = x
        curr_value=[]
    else:
        curr_value.append(x)
if curr_key is not None:
    temp[curr_key]=curr_value

print(temp)




#if isinstance(x,str): it means any where if data is string do something


