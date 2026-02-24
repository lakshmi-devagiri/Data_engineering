def uniq(lst):
    uni=[]
    dup=[]
    for x in lst:
        if x not in uni:
            uni.append(x)
            diff = list(set(lst) - set(uni))
        else:
            dup.append(diff)
    return dup
lst=[1,1,2,3,4,5,5,4,9]
res=uniq(lst)
print(res)