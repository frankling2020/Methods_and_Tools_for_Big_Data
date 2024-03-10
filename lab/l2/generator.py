import numpy as np
import csv
import sys

w = open('scores.csv', 'w', encoding='utf8', newline="")
file = csv.writer(w)
MAX_REPEAT = int(sys.argv[1]) # 10, 100, 500, 1000, 2000
TOTAL_NAMES = 5000

# print("test" + str(MAX_REPEAT))

with open("./firstnames.txt", "r") as firstname:
    with open("./lastnames.txt", "r") as lastname:
        x = TOTAL_NAMES
        while (x > 0):
            x -= 1
            f = firstname.readline().strip("\n")
            l = lastname.readline().strip("\n")
            name = f + " " + l
            id = np.random.randint(10**10, 10**11, dtype=np.int64())
            for _ in range(np.random.randint(MAX_REPEAT)):
                score = np.random.randint(0, 101)
                file.writerow([name, id, score]) 
w.close()

