"""Quick check VM MySQL connection"""
import pymysql
c = pymysql.connect(host='192.168.184.200',user='root',password='123456',port=3306,database='ubanalysis',connect_timeout=5)
cu = c.cursor()
cu.execute('SELECT COUNT(*) FROM demo1')
print('demo1:', cu.fetchone()[0], 'rows')
cu.execute('SELECT province, cnt FROM demo1 LIMIT 3')
for r in cu.fetchall():
    print(' ', r[0], r[1])
c.close()
print('OK')
