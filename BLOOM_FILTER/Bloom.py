import random

#this is a simple implementation of bloom filter on 

bloom =[0]*32
def bloomfilter(a):
    # 3 hash functions
    hash1 = (a * 7) % 32
    hash2=(a*(a-3))%32
    hash3=(a*pow(a,2))%32
    count =0
    if bloom[hash1]==1:
        count+=1
    if bloom[hash2]==1:
        count+=1
    if bloom[hash3]==1:
        count+=1
    bloom[hash1]=1
    bloom[hash2]=1
    bloom[hash3]=1
    if count==3:
        print("100% Present")
    else:
        print("Maybe Present")

if __name__=="__main__":
    for i in range(10):
        a=int(input("enter:"))
        bloomfilter(a)
