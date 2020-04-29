import csv

year = 1987

with open(f'PlayerStats/{year}_totals.csv', 'r') as f:
    csv_read = csv.reader(f)
    i = 0
    for row in csv_read:
        if i == 0:
            i += 1
            continue
        r = []
        for entry in row:
            if '.' in entry:
                try:
                    r.append(float(entry))
                except:
                    r.append(entry)
            else:
                try:
                    r.append(int(entry))
                except:
                    r.append(entry)
        id_key = r[1].replace(' ', '_') + '_' + str(year) + '_' + str(r[0])
        print(id_key, year, r)
