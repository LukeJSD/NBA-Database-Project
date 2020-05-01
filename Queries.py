import pg8000
import getpass

# Enter database credentials
user = input('Username: ')
secret = getpass.getpass(prompt='Password: ')
# Connect to database
print('Trying to connect...')
db = pg8000.connect(user=user, password=secret, host='bartik.mines.edu', database='csci403')
cursor = db.cursor()
print('Connected')

cursor.execute('SELECT * FROM nba_standings AS d JOIN (SELECT * FROM nba_abbreviations AS c JOIN (SELECT * FROM nba_tot_seasons AS a JOIN nba_adv_seasons AS b ON a.id=b.id) AS j2 ON abrev=a.tm;')
cursor.execute('SELECT * FROM nba_standings AS d JOIN (SELECT * FROM nba_abbreviations AS c JOIN (SELECT * FROM nba_tot_seasons AS a JOIN nba_adv_seasons AS b ON a.id=b.id) AS j2 ON abrev=a.tm) AS j2 ON d.Team=c.Team;')
