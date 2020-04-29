import pg8000
import getpass
import os
from os import path
import csv
from collections.abc import Iterable


# This takes a while on the first run

# Connect to database
user = 'ljulian'
secret = 'fleabagS2!'
print('Trying to connect...')
db = pg8000.connect(user=user, password=secret, host='bartik.mines.edu', database='csci403')
cursor = db.cursor()
print('Connected')

cursor.execute("DROP TABLE IF EXISTS nba_standings;")
print('nba_standings deleted...', end='')
cursor.execute("DROP TABLE IF EXISTS nba_drafts;")
print('nba_drafts deleted...', end='')

db.commit()
print('Tables cleared')

cursor.execute("CREATE TABLE nba_standings (ID VARCHAR(50), Year INTEGER, R INTEGER, Tm VARCHAR(50), W INTEGER, L INTEGER, Wpercent NUMERIC(4,3), GB NUMERIC(3,1), PSperG NUMERIC(4,1), PAperG NUMERIC(4,1), SRS NUMERIC(4,2));")
cursor.execute("CREATE TABLE nba_drafts (ID VARCHAR(50), Year INTEGER, R INTEGER, Pk INTEGER, Tm VARCHAR(50), Player VARCHAR(50), College VARCHAR(50), Yrs INTEGER, G INTEGER, MP INTEGER, PTS INTEGER, TRB INTEGER, AST INTEGER, FGpercent NUMERIC(4,3), ThrP NUMERIC(5,1), FTpercent NUMERIC(4,3), MPperGm NUMERIC(7,3), PTSperGM NUMERIC(7,3), TRBperGm NUMERIC(7,3), ASTperGM NUMERIC(7,3), WS NUMERIC(5,2), WSper48 NUMERIC(7,3), BPM NUMERIC(3,1), VORP NUMERIC(4,1));")

db.commit()

draft_errors = []
standings_errors = []

for year in [y for y in range(1956, 2021)]:
    print('Draft...', end='')
    copy_command = 'INSERT INTO nba_drafts VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    with open(f'Drafts/{year}_draft.csv', 'r') as f:
        csv_read = csv.reader(f)
        header = []
        header = True
        note = ''
        for row in csv_read:
            if header:
                header = False
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
            id_key = r[3].replace(' ', '_') + '_' + str(year) + '_' + str(r[1])
            try:
                cursor.execute(copy_command, (id_key, year, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21]))
            except Exception as e:
                print('dft', e)
                print(id_key, year, r)
                draft_errors.append([id_key, year, r])
                cursor.execute('ROLLBACK;')

    print('Standings...', end='')
    copy_command = 'INSERT INTO nba_standings VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    with open(f'Standings/standings_{year}.csv', 'r') as f:
        csv_read = csv.reader(f)
        header = []
        header = True
        note = ''
        for row in csv_read:
            if header:
                header = False
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
            id_key = r[1].replace(' ', '_') + '_' + str(year)
            try:
                cursor.execute(copy_command, (id_key, year, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]))
            except Exception as e:
                print('std', e)
                print(id_key, year, r)
                draft_errors.append([year, r])
                cursor.execute('ROLLBACK;')

    print(year, 'loaded')

    db.commit()

db.commit()

print('Done')

with open('error_inserts.csv', 'a') as f:
    for e in draft_errors:
        f.write('tot')
        f.write(','+str(e[0]))
        f.write(','+str(e[1]))
        for i in e[2]:
            f.write(','+str(i))
        f.write('\n')
    for e in standings_errors:
        f.write('game')
        f.write(','+str(e[0]))
        for i in e[1]:
            f.write(','+str(i))
        f.write('\n')

# If hanging:
# SELECT query,pid,usesysid,usename,state FROM pg_stat_activity WHERE state LIKE '%idle%;
# SELECT pg_terminate_backend(pid);
