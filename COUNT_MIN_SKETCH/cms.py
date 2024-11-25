#i made 4 hash function for counting the frequency of a number in a stream of numbers

hashtable = [[0] * 5 for _ in range(4)]  

def cms(a):
    hash1 = pow(a * 5, 3) % 5
    hash2 = (a * 2) % 5
    hash3 = (a * 45 - 342) % 5
    hash4 = ((a + 23) - a * 2) % 5

    hash1 = abs(hash1)
    hash2 = abs(hash2)
    hash3 = abs(hash3)
    hash4 = abs(hash4)


    hashtable[0][hash1] += 1
    hashtable[1][hash2] += 1
    hashtable[2][hash3] += 1
    hashtable[3][hash4] += 1

def get_count(a):
    hash1 = pow(a * 5, 3) % 5
    hash2 = (a * 2) % 5
    hash3 = (a * 45 - 342) % 5
    hash4 = ((a + 23) - a * 2) % 5

    hash1 = abs(hash1)
    hash2 = abs(hash2)
    hash3 = abs(hash3)
    hash4 = abs(hash4)

    arr = [
        hashtable[0][hash1],
        hashtable[1][hash2],
        hashtable[2][hash3],
        hashtable[3][hash4],
    ]
    print("Probable count is", min(arr))

if __name__ == "__main__":
    a = int(input("Enter number to count (press -1 to exit): "))
    while a != -1:
        cms(a)
        a = int(input("Enter number to count (press -1 to exit): "))
    
    b = int(input("To get the count of (press -1 to exit): "))
    while b != -1:
        get_count(b)
        b = int(input("To get the count of (press -1 to exit): "))
