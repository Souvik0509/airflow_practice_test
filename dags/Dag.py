import requests 
import pandas as pd 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# default_args = {
#      "owner" : "souvik",
#      "depends_on_past" : False,
#      "start_date": datetime(2025,5,26),
#      "retries": 1,
#      "retry_delay": timedelta(minutes=5)
# }
 
 #dag = DAG(
#    'fetch_and_store_online_books',
#     default_args = default_args,
#     description = 'A normal dag to fetch books information from online store and store it in postgres',
#     schedule_interval = timedelta(minutes=2)
# )

# default_args = {
#      "owner" : "souvik",
#      "depends_on_past" : False,
#      "start_date": datetime(2025,5,26),
#      "retries": 1,
#      "retry_delay": timedelta(minutes=5)
# }
 
# dag = DAG(
#     'fetch_books_from_google_api',
#     default_args = default_args,
#     description = 'A normal dag to fetch books information from online store and store it in postgres',
#     schedule_interval = timedelta(minutes=2)
# )


# headers = {
#     "Referer": "https://www.amazon.com/",
#     "Sec-Ch-Ua": '"Chromium";v="107", "Not=A?Brand";v="24"',
#     "Sec-Ch-Ua-Mobile": "?0",
#     "Sec-Ch-Ua-Platform": "Windows",
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                   "AppleWebKit/537.36 (KHTML, like Gecko) "
#                   "Chrome/107.0.0.0 Safari/537.36"
# }

# base_url = f"https://www.amazon.com/s?k=data+engineering+books"
# page = 2
# url = f"{base_url}&page={page}"
# response = requests.get(url, headers=headers)
# if response.status_code == 200:
#     print(response.text[:1000])



def fetch_books_from_google_api(query,ti, max_results=10,):
    base_url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "maxResults": max_results
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        books = response.json().get("items", [])
        book_list = []
        for book in books:
            info = book.get("volumeInfo", {})
            book_list.append({
                "Title": info.get("title"),
                "Authors": ", ".join(info.get("authors", [])),
                "PublishedDate": info.get("publishedDate"),
                "Description": info.get("description"),
                "PageCount": info.get("pageCount"),
                "Categories": ", ".join(info.get("categories", [])),
                "AverageRating": info.get("averageRating"),
                "RatingsCount": info.get("ratingsCount"),
                "Thumbnail": info.get("imageLinks", {}).get("thumbnail")
            })
    else:
        print(f"Failed to fetch data: {response.status_code}")
    
    df= pd.DataFrame(book_list)
    df.drop_duplicates(subset=['Title'],inplace=True)
    ti.xcom_push(key='books_data', value=df.to_dict('records'))


def store_books_in_postgres(ti):
    book_data = ti.xcom_pull(key='books_data', task_ids='fetch_books_task')
    if not book_data:
        raise ValueError("No book data found to store.")
        
    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    insert_sql =  """
        INSERT INTO online_books 
        (Title, Authors, PublishedDate, Description, PageCount, Categories, AverageRating, RatingsCount, Thumbnail)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """   
    for _, row in pd.DataFrame(book_data).iterrows():
        cursor.execute(
            insert_sql,
            (row['Title'], row['Authors'], row['PublishedDate'], row['Description'], row['PageCount'], row['Categories'], row['AverageRating'], row['RatingsCount'], row['Thumbnail'])
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully")


default_args = {
     "owner" : "souvik",
     "depends_on_past" : False,
     "start_date": datetime(2025,5,26),
     "retries": 1,
     "retry_delay": timedelta(minutes=5)
}
 
dag = DAG(
    'fetch_books_from_google_api',
    default_args = default_args,
    description = 'A normal dag to fetch books information from online store and store it in postgres',
    schedule_interval = timedelta(minutes=2)
)



fetch_books_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=fetch_books_from_google_api,
    op_args=["data engineering"],
    op_kwargs={"max_results": 50}, 
    dag=dag,
) 


create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS online_books (
        Title TEXT,
        Authors TEXT,
        PublishedDate TEXT,
        Description TEXT,
        PageCount INT,
        Categories TEXT,
        AverageRating FLOAT,
        RatingsCount INT,
        Thumbnail TEXT
    );
    """,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=store_books_in_postgres,
    dag=dag,
)

fetch_books_task >> create_table_task >> insert_data_task


