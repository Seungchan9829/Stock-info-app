from db.db import get_connection

def get_credit_finance_data(start_date, end_date):
    # DB 연결
    conn = get_connection()
    cur = conn.cursor()

    # 쿼리 
    query = "SELECT date, loan_total, loan_kospi, loan_kosdaq from credit_transactions WHERE date BETWEEN %s AND %s"
    cur.execute(query, (start_date, end_date))

    #결과
    result = cur.fetchall()
    
    # 종료
    cur.close()
    conn.close()
    
    return result


def get_kospi_price_data(start_date, end_date):
    # DB 연결
    conn = get_connection()
    cur = conn.cursor()

    # 쿼리
    query = "SELECT date, kospi_index from kospi_summary WHERE date BETWEEN %s AND %s"
    cur.execute(query, (start_date, end_date))

    result = cur.fetchall()

    #종료
    cur.close()
    conn.close()

    return result


get_kospi_price_data("2025-01-25", "2025-06-25")