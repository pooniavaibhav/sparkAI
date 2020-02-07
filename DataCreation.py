from random import randint
j = 0
for i in range(50000):
    data = []
    data.append(j)
    data.append((randint(0, 9)))
    j = j + 1
    with open("/home/vaibhav/Desktop/linear_data40k.csv","a") as file:
        file.write(str(data))
        file.write("\n")

