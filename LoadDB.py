import pg8000
import getpass
import os
from os import path
import csv
import nba_scrape

# This takes a while on the first run
exec('nba_scrape')

# Connect to database
user = input('Username: ')
secret = getpass.getpass()
print('Trying to connect...')
db = pg8000.connect(user=user, password=secret, host='bartik.mines.edu', database='csci403')
cursor = db.cursor()
print('Connected')

cursor.execute("DROP TABLE IF EXISTS nba_tot_seasons;")
print('nba_tot_seasons deleted...', end='')
cursor.execute("DROP TABLE IF EXISTS nba_adv_seasons;")
print('nba_adv_seasons deleted...', end='')
cursor.execute("DROP TABLE IF EXISTS nba_games;")
print('nba_games deleted...', end='')
cursor.execute("DROP TABLE IF EXISTS nba_abbreviations;")
print('nba_abbreviations deleted...', end='')

db.commit()
print('Tables cleared')

cursor.execute("CREATE TABLE nba_abbreviations_temp (Name VARCHAR(50), Abbreviations VARCHAR(4));")
cursor.execute("CREATE TABLE NBA_tot_seasons (ID VARCHAR(50) PRIMARY KEY, Year INTEGER, R INTEGER, Player VARCHAR(50), Pos VARCHAR(5), BasePos VARCHAR(2), Age NUMERIC(4,1), Tm VARCHAR(50), G NUMERIC(4,1), GS NUMERIC(4,1), MP NUMERIC(6,1), FG NUMERIC(5,1), FGA NUMERIC(5,1), FGpercent NUMERIC(4,3), ThrP NUMERIC(5,1), ThrPA NUMERIC(5,1), ThrPpercent NUMERIC(4,3), TwoP NUMERIC(5,1), TwoPA NUMERIC(5,1), TwoPpercent NUMERIC(4,3), eFGpercent NUMERIC(4,3), FT NUMERIC(5,1), FTA NUMERIC(5,1), FTpercent NUMERIC(4,3), ORB NUMERIC(5,1), DRB NUMERIC(5,1), TRB NUMERIC(5,1), AST NUMERIC(5,1), STL NUMERIC(4,1), BLK NUMERIC(4,1), TOV NUMERIC(5,1), PF NUMERIC(4,1), PTS NUMERIC(6,1));")
cursor.execute("CREATE TABLE nba_adv_seasons(ID VARCHAR(50) PRIMARY KEY, Year INTEGER, R INTEGER, Player VARCHAR(50), Pos VARCHAR(5), BasePos VARCHAR(2), Age NUMERIC(4,1), Tm VARCHAR(50), G NUMERIC(4,1), MP NUMERIC(6,1), PER NUMERIC(4,1), TSpercent NUMERIC(4,3), ThrPAr NUMERIC(4,3), FTr NUMERIC(4,3), ORBpercent NUMERIC(4,1), DRBpercent NUMERIC(4,1), TRBpercent NUMERIC(4,1), ASTpercent NUMERIC(4,1), STLpercent NUMERIC(4,1), BLKpercent NUMERIC(4,1), TOVpercent NUMERIC(4,1), USG NUMERIC(4,1), zeroA NUMERIC(2,1), OWS NUMERIC(4,1), DWS NUMERIC(4,1), WS NUMERIC(4,1), WSper48 NUMERIC(2,1), OBPM NUMERIC(4,1), DBPM NUMERIC(4,1), BPM NUMERIC(4,1), VORP NUMERIC(2,1));")
cursor.execute("CREATE TABLE nba_games(Year INTEGER, Date DATE, href VARCHAR(50), Visitor VARCHAR(50), VPts INTEGER, Home VARCHAR(50), HPts INTEGER, OT VARCHAR(4), Attend INTEGER, Notes VARCHAR(100));")
cursor.execute("CREATE TABLE nba_abbreviations(Team VARCHAR(50), Abrev VARCHAR(4));")

copy_command = 'INSERT INTO nba_abbreviations VALUES (%s, %s);'
with open(f'abreviations.csv', 'r') as f:
    for row in f.readlines()[1:]:
        r = row.split(',')
        cursor.execute(copy_command, (r[0], r[1]))

db.commit()

tot_errors = []
adv_errors = []
game_errors = []

for year in [y for y in range(1956, 2021)]:
    print('Totals...',end='')
    copy_command = 'INSERT INTO NBA_tot_seasons VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
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
            try:
                cursor.execute(copy_command, (id_key, year, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21], r[22], r[23], r[24], r[25], r[26], r[27], r[28], r[29], r[30]))
            except Exception as e:
                print('tot', e)
                print(id_key, year, r)
                tot_errors.append([id_key, year, r])
                cursor.execute('ROLLBACK;')

    print('Advanced...', end='')
    copy_command = 'INSERT INTO NBA_adv_seasons VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    with open(f'PlayerStats/{year}_advanced.csv', 'r') as f:
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
            try:
                cursor.execute(copy_command, (id_key, year, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21], r[22], r[23], r[24], r[25], r[26], r[27], r[28]))
            except Exception as e:
                print('adv', e)
                print(id_key, year, r)
                adv_errors.append([id_key, year, r])
                cursor.execute('ROLLBACK;')

    print('Games...', end='')
    copy_command = 'INSERT INTO nba_games VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    with open(f'Games/games{year}.csv', 'r') as f:
        csv_read = csv.reader(f)
        header = []
        i = 0
        note = ''
        for row in csv_read:
            if 'Playoffs' in row:
                note = 'Playoffs'
                continue
            if i == 0:
                columns = row
                for h in ['Date', 'href', 'Visitor', 'Home', 'Attend', 'Notes']:
                    for j in range(len(row)):
                        if h in row[j]:
                            if h == 'Attend':
                                header.append(j - 1)
                            header.append(j)
                            if h == 'Visitor' or h == 'Home':
                                header.append(j + 1)
                i += 1
                standard_len = len(row)
                continue
            r = []
            for index in header:
                entry = row[index]
                if 'Attend' in columns[index]:
                    entry = entry.replace(',', '')
                try:
                    r.append(int(entry))
                except Exception as e:
                    r.append(entry)
            try:
                if type(r[7]) == str:
                    cursor.execute(copy_command, (year, r[0], r[1], r[2], r[3], r[4], r[5], r[6], -1, r[8] + note))
                else:
                    cursor.execute(copy_command, (year, r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8] + note))
            except Exception as e:
                print('Games', e)
                print(year, r)
                game_errors.append([note, year, r])
                cursor.execute('ROLLBACK;')

    if path.exists(f'PlayerStats/{year}_advanced.csv'):
        os.remove(f'PlayerStats/{year}_advanced.csv')
    if path.exists(f'PlayerStats/{year}_totals.csv'):
        os.remove(f'PlayerStats/{year}_totals.csv')
    if path.exists(f'Games/games{year}.csv'):
        os.remove(f'Games/games{year}.csv')

    print(year, 'loaded')

    db.commit()

if len(os.listdir('PlayerStats')) == 0:
    os.rmdir('PlayerStats')
if len(os.listdir('Games')) == 0:
    os.rmdir('Games')

cursor.execute("UPDATE nba_games SET Attend=NULL WHERE Attend=-1;")
cursor.execute("UPDATE nba_games SET OT=NULL WHERE OT='';")

db.commit()

print('Done')

with open('error_inserts.csv', 'a') as f:
    for e in tot_errors:
        f.write('tot')
        f.write(','+str(e[0]))
        f.write(','+str(e[1]))
        for i in e[2]:
            f.write(','+str(i))
        f.write('\n')
    for e in adv_errors:
        f.write('adv')
        f.write(','+str(e[0]))
        f.write(','+str(e[1]))
        for i in e[2]:
            f.write(','+str(i))
        f.write('\n')
    for e in game_errors:
        f.write('game')
        f.write(','+str(e[0]))
        f.write(','+str(e[1]))
        for i in e[2]:
            f.write(','+str(i))
        f.write('\n')

# If hanging:
# SELECT query,pid,usesysid,usename,state FROM pg_stat_activity WHERE state LIKE '%idle%;
# SELECT pg_terminate_backend(pid);
