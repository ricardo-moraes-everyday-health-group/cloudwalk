from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from util.templates import (delete_all_countries_query, delete_all_gdp_query,
                            delete_all_query, html_full, html_table_col,
                            html_table_header, html_table_row,
                            insert_countries_query, insert_gdp_query,
                            insert_query, report_query, years_query)


def get_next_page_cursor(response):
    current_page = response.json()[0].get("page", 1)
    total_pages = response.json()[0].get("pages", 1)
    per_page = str(response.json()[0].get("per_page"))
    next_page_cursor = None
    if current_page < total_pages:
        next_page = str(current_page+1)
        next_page_cursor = dict(data={"format": "json", "page": next_page, "per_page": per_page})
        print(next_page_cursor)
    return next_page_cursor

def write_to_database(ti):
    
    import json

    from airflow.hooks.postgres_hook import PostgresHook
    
    lines_to_insert = []
    
    r = ti.xcom_pull(task_ids=["load_api_data"])
    
    for cnk in r[0]:
        j = json.loads(cnk)
        for jj in j[1]:
            record_to_insert = (jj['country']['id'],
                                jj['country']['value'],
                                jj['countryiso3code'],
                                jj['date'],
                                jj['value'],
                                jj['unit'],
                                jj['obs_status'],
                                jj['decimal'],)
            print(record_to_insert, type(record_to_insert))
            lines_to_insert.append(record_to_insert)
            
    pg_hook = PostgresHook(postgres_conn_id="DB_GDP")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    records_list_template = ','.join(['%s'] * len(lines_to_insert))
    
    cursor.execute(delete_all_query)
    
    cursor.execute(insert_query.format(records_list_template=records_list_template)
                   , lines_to_insert)
    
    connection.commit()
    
    cursor.close()
    
    connection.close()

def write_to_target_tables():
    
    from airflow.hooks.postgres_hook import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id="DB_GDP")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    cursor.execute(delete_all_gdp_query)
    cursor.execute(delete_all_countries_query)
    cursor.execute(insert_countries_query)
    cursor.execute(insert_gdp_query)
    
    connection.commit()
    
    cursor.close()
    
    connection.close()
    
def generate_report():
    
    from airflow.hooks.postgres_hook import PostgresHook
            
    pg_hook = PostgresHook(postgres_conn_id="DB_GDP")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    cursor.execute(years_query)
        
    y = cursor.fetchall()
    
    list_of_categories = []
    list_of_columns = []
    for r in y:
        
        list_of_categories.append('"{cat}" DECIMAL'.format(cat=r[0]))
        list_of_columns.append('piv_tb."{col_name}"'.format(col_name=r[0]))

    categories = ",".join(list_of_categories)
    cols = ",".join(list_of_columns)
    
    print(categories)
    print(cols)
    
    rq = report_query.format(categories=categories, cols=cols, years_query=years_query)

    cursor.execute(rq)
    
    t_headers = [html_table_header.format(h_desc=field_md[0]) for field_md in cursor.description]
    t_header = "".join(t_headers)
    
    t_lines = ''
    for row in cursor.fetchall():
        t_line = ''
        for val in row:
            if val:
                t_line = t_line + html_table_col.format(c_desc=val)
            else:
                t_line = t_line + html_table_col.format(c_desc='-')
                
        t_lines = t_lines + html_table_row.format(r_desc=t_line)
    
    cursor.close()
    
    connection.close() 

    with open('/opt/airflow/output/gdp_report.html', "w") as f:
        f.write(html_full.format(t_header=t_header, t_rows=t_lines, qry=rq))


with DAG(
    dag_id="gross_domestic_product",
    schedule_interval=None,
    tags=["test"],
    start_date=datetime(year=2024, month=5, day=1),
    ) as dag:

    task_check_api = HttpSensor(
        task_id="check_api",
        method="GET",
        http_conn_id="API_WORLDBANK_INDICATOR",
        endpoint="/country",
        deferrable=True,
        poke_interval=5,
        timeout=50
    )
    
    task_load_api_data = HttpOperator(
        task_id="load_api_data",
        method="GET",
        http_conn_id="API_WORLDBANK_INDICATOR",
        endpoint="/country/{{ conn.API_WORLDBANK_INDICATOR.extra_dejson.countries }}/indicator/NY.GDP.MKTP.CD",
        data={"format": "json", "page": "1", "per_page": "{{ conn.API_WORLDBANK_INDICATOR.extra_dejson.per_page }}"},
        pagination_function=get_next_page_cursor,
    )
    
    task_write_data = PythonOperator(
        task_id="write_api_data",
        python_callable=write_to_database
    )

    task_write_tgt_tables = PythonOperator(
        task_id="write_to_target_tables",
        python_callable=write_to_target_tables
    )
    
    task_generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report
    )

task_check_api.set_downstream(task_load_api_data)
task_load_api_data.set_downstream(task_write_data)
task_write_data.set_downstream(task_write_tgt_tables)
task_write_tgt_tables.set_downstream(task_generate_report)


