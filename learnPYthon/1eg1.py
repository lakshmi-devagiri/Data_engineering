lst=[1,3,9,5,0,-1,-9,7,-3]
temp = []
#for x in lst[::-1]:
for x in lst:
    if x==0:
        temp.append(None)
    elif x%2==0:
        temp.append(x*x)
    elif x%2 !=0:
        temp.append(x*x*x)
print("input data:", lst)
print(temp)



#input() always return in the form

st = "10 20 30 40 11 40"
nums = st.split(" ")
print(nums)
'''process = [int(x) for x in num]
print(process)

'''
print("before process",nums)

def testname(num):
    return (int(num) *int(num))

res=list(map(testname,nums))
#res=list(map(lambda x:int(x)*int(x),nums))
print('after process',res)


