import sqlite3

conn = sqlite3.connect('output/db/agri_schemes.db')
cursor = conn.cursor()

# Check curated schemes with their confidence scores
cursor.execute('''
SELECT scheme_id, scheme_name, scheme_type, confidence_score, curated_flag 
FROM curated_schemes 
ORDER BY confidence_score DESC
''')

print('Curated schemes (sorted by confidence):')
for row in cursor.fetchall():
    print(f'  ID: {row[0]}, Name: {row[1]}, Type: {row[2]}, Score: {row[3]}, Curated: {row[4]}')

# Check which ones have score < 45
cursor.execute('''
SELECT COUNT(*) FROM curated_schemes WHERE confidence_score < 45
''')
print(f'\nSchemes with score < 45: {cursor.fetchone()[0]}')

# Check which ones would be returned by /api/schemes
cursor.execute('''
SELECT COUNT(*) FROM curated_schemes WHERE confidence_score >= 45 AND curated_flag = 1
''')
print(f'Schemes returned by /api/schemes (score >= 45, curated_flag=1): {cursor.fetchone()[0]}')

conn.close()
