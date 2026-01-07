import sqlite3

def search_vault():
    # Connect to the database you created
    conn = sqlite3.connect('goodreads_production.db')
    cursor = conn.cursor()

    print("\n📚 WELCOME TO THE GOODREADS VAULT 📚")
    keyword = input("🔎 Enter a book title or keyword (e.g., 'Potter', 'Space', 'Data'): ")

    # This SQL query searches for your keyword anywhere in the title
    # and sorts them by the highest rating first
    query = """
    SELECT title, avg_rating 
    FROM books 
    WHERE title LIKE ? 
    ORDER BY avg_rating DESC 
    LIMIT 10
    """
    
    cursor.execute(query, (f'%{keyword}%',))
    results = cursor.fetchall()

    print(f"\n--- Top 10 Rated Matches for '{keyword}' ---")
    if not results:
        print("❌ No books found. Try a different keyword!")
    else:
        for row in results:
            # This prints the rating and title neatly
            print(f"⭐ {row[1]} | {row[0]}")
    
    conn.close()

if __name__ == "__main__":
    search_vault()