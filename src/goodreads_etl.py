import sqlite3
import os
import logging
import argparse
import pandas as pd
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# 1. CONFIGURABILITY & PERFORMANCE TRACKING
parser = argparse.ArgumentParser(description="Production-Grade Goodreads ETL Pipeline")
parser.add_argument('--folder', default='data/DATA_CSV', help='Source folder for CSVs')
parser.add_argument('--db', default='goodreads_production.db', help='Target Database')
parser.add_argument('--workers', type=int, default=4, help='Parallel threads for processing')
args = parser.parse_args()

logging.basicConfig(
    filename='goodreads_log.txt',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s,%(levelname)s,%(message)s'
)

RATING_MAP = {
    'it was amazing': 5, 'really liked it': 4, 'liked it': 3,
    'it was ok': 2, 'did not like it': 1, 'this user marked the book as "to-read"': 0
}

# 2. HELPER FOR DB INTEGRITY
def insert_ignore(table, conn, keys, data_iter):
    sql = f"INSERT OR IGNORE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?'] * len(keys))})"
    conn.executemany(sql, data_iter)

# 3. VISUAL PREVIEW (Legacy Requirement)
def peek_at_data(df, filename):
    logging.info(f"\n{'='*20} TABLE VIEW: {filename} {'='*20}")
    row_fmt = "%-8s | %-40s | %-20s"
    logging.info(row_fmt % ("ID", "NAME/TITLE", "RAW RATING"))
    logging.info("-" * 75)
    for _, row in df.head(5).iterrows():
        id_val = str(row.iloc[0])
        name = (str(row.iloc[1])[:37] + '...') if len(str(row.iloc[1])) > 37 else str(row.iloc[1])
        rating = str(row.iloc[2])
        logging.info(row_fmt % (id_val, name, rating))
    logging.info("=" * 75 + "\n")

# 4. DATA VALIDATION & TRANSFORMATION
def process_single_file(filename, db_path):
    path = os.path.join(args.folder, filename)
    conn = sqlite3.connect(db_path)
    total_processed = 0
    start_time = time.time()
    
    try:
        for chunk in pd.read_csv(path, chunksize=100000, low_memory=False, encoding='utf-8', on_bad_lines='skip'):
            if filename.startswith('book'):
                # VALIDATION: Ensure Id is numeric and Rating is float
                chunk['Id'] = pd.to_numeric(chunk['Id'], errors='coerce')
                chunk['Rating'] = pd.to_numeric(chunk['Rating'], errors='coerce')
                chunk = chunk.dropna(subset=['Id', 'Name']) # Quarantine bad data
                
                clean_chunk = chunk[['Id', 'Name', 'Rating']].rename(
                    columns={'Id': 'book_id', 'Name': 'title', 'Rating': 'avg_rating'}
                )
                clean_chunk.to_sql('books', conn, if_exists='append', index=False, method=insert_ignore)
                total_processed += len(clean_chunk)
                type_label = "books"

            elif filename.startswith('user_rating'):
                # VALIDATION: Map ratings and handle missing users
                chunk['rating_int'] = chunk['Rating'].str.lower().str.strip().map(RATING_MAP).fillna(0)
                chunk = chunk.dropna(subset=['Name', 'ID'])
                
                clean_chunk = chunk[['Name', 'ID', 'Rating', 'rating_int']].rename(
                    columns={'Name': 'book_title', 'ID': 'user_id', 'Rating': 'rating_text'}
                )
                clean_chunk.to_sql('ratings', conn, if_exists='append', index=False)
                total_processed += len(clean_chunk)
                type_label = "ratings"
        
        elapsed = time.time() - start_time
        logging.info(f"✓ Processed {total_processed} {type_label} from {filename} ({total_processed/elapsed:.0f} rows/sec)")
    finally:
        conn.close()
    return total_processed

# 5. ORCHESTRATION
def run_production_etl():
    start_total = time.time()
    print("🚀 Starting Production-Grade ETL...")
    
    # Init Schema & Performance Indexes
    conn = sqlite3.connect(args.db)
    conn.execute('CREATE TABLE IF NOT EXISTS books (book_id TEXT PRIMARY KEY, title TEXT, avg_rating REAL)')
    conn.execute('DROP TABLE IF EXISTS ratings')
    conn.execute('''CREATE TABLE ratings (
        rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_title TEXT, user_id TEXT, rating_text TEXT, rating_int INTEGER
    )''')
    
    # PREVIEW LOGGING (Required first)
    target_book = os.path.join(args.folder, 'book1-100k.csv')
    target_rating = os.path.join(args.folder, 'user_rating_0_to_1000.csv')
    if os.path.exists(target_book): peek_at_data(pd.read_csv(target_book, nrows=5), 'book1-100k.csv')
    if os.path.exists(target_rating): peek_at_data(pd.read_csv(target_rating, nrows=5), 'user_rating_0_to_1000.csv')
    conn.close()

    # PARALLEL PROCESSING
    csv_files = sorted([f for f in os.listdir(args.folder) if f.endswith('.csv')])
    print(f"Parallelizing {len(csv_files)} files across {args.workers} workers...")
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        executor.map(lambda f: process_single_file(f, args.db), csv_files)

    # POST-PROCESSING: INDEXING (The Performance Secret)
    print("⚡ Optimizing database indexes...")
    conn = sqlite3.connect(args.db)
    conn.execute('CREATE INDEX IF NOT EXISTS idx_book_title ON books(title)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_rating_title ON ratings(book_title)')

    # INSIGHTS (Now lightning fast due to indexes)
    books_count = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    ratings_count = conn.execute("SELECT COUNT(*) FROM ratings").fetchone()[0]

    print("\n" + "════" * 12 + "\n📊 PRODUCTION ETL INSIGHTS\n" + "════" * 12)
    print(f"Total Database: {books_count:,} Books | {ratings_count:,} Ratings.")
    
    query = '''
        SELECT b.title, COUNT(r.rating_id) as reviews 
        FROM books b JOIN ratings r ON b.title = r.book_title 
        GROUP BY b.title ORDER BY reviews DESC LIMIT 5
    '''
    df_top = pd.read_sql_query(query, conn)
    for _, row in df_top.iterrows():
        print(f"• {row['title'][:40]:<40} | {int(row['reviews']):,} reviews")

    total_time = time.time() - start_total
    print(f"\n✅ Deployment Ready. Total Time: {total_time:.2f}s")
    logging.info(f"=== ETL COMPLETE | TOTAL TIME: {total_time:.2f}s ===")
    conn.close()

# 6. ===GENERATE HTML DASHBOARD ===
    html_content = f"""
    <html>
    <head>
        <title>Goodreads ETL Dashboard</title>
        <style>
            body {{ font-family: sans-serif; margin: 40px; background: #f4f4f9; color: #333; }}
            .card {{ background: white; padding: 20px; border-radius: 10px; shadow: 0 4px 8px rgba(0,0,0,0.1); }}
            h1 {{ color: #2c3e50; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #34495e; color: white; }}
            .stat-box {{ display: inline-block; padding: 15px; background: #3498db; color: white; border-radius: 5px; margin-right: 10px; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>📊 Goodreads ETL Production Report</h1>
            <div class="stat-box"><strong>Total Books:</strong> {books_count:,}</div>
            <div class="stat-box"><strong>Total Ratings:</strong> {ratings_count:,}</div>
            <div class="stat-box"><strong>Execution Time:</strong> {total_time:.2f}s</div>
            
            <h2>🏆 Most Reviewed Books</h2>
            {df_top.to_html(index=False, classes='table')}
            <p><i>Report generated on: {time.ctime()}</i></p>
        </div>
    </body>
    </html>
    """
    
    with open("dashboard.html", "w") as f:
        f.write(html_content)
    print("\n🌐 Dashboard generated: Check dashboard.html")
    # === END HTML GENERATION ===

if __name__ == "__main__":
    run_production_etl()