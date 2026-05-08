import sqlite3

conn = sqlite3.connect('output/db/agri_schemes.db')
cursor = conn.cursor()

# Check tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('Tables:')
for row in cursor.fetchall():
    print(f'  {row[0]}')

# Check counts
cursor.execute('SELECT COUNT(*) FROM master_schemes')
print(f'\nMaster schemes: {cursor.fetchone()[0]}')

cursor.execute('SELECT COUNT(*) FROM curated_schemes')
print(f'Curated schemes: {cursor.fetchone()[0]}')

cursor.execute('SELECT COUNT(*) FROM schemes')
print(f'Total schemes: {cursor.fetchone()[0]}')

# Check a sample scheme from master_schemes
cursor.execute('SELECT scheme_id, scheme_name, search_blob FROM master_schemes LIMIT 3')
print('\nSample master schemes:')
for row in cursor.fetchall():
    print(f'  ID: {row[0]}, Name: {row[1]}, Blob: {str(row[2])[:60]}...' if row[2] else f'  ID: {row[0]}, Name: {row[1]}, Blob: NULL')

conn.close()
