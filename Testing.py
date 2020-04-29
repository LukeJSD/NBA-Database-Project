import csv

for i in range(9):
    print(f'r[{i}]', end=', ')


for year in range(1956, 2021):
    with open(f'Standings/standings_{year}.csv', 'r') as f:
        csv_read = csv.reader(f)
        i = 0
        for row in csv_read:
            if i == 0:
                print(year,len(row))
                i += 1
                break

