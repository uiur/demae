# split an array into n size groups
def split_size(a, n):
    return [a[(len(a) * i // n):(len(a) * (i+1) // n)] for i in range(n)]
