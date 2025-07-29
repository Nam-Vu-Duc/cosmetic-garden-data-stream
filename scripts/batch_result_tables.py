import psycopg2
from dotenv import load_dotenv

load_dotenv()

# postgres
conn = psycopg2.connect(
    dbname="admin",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)

def batch_result_tables() -> None:
    cur = conn.cursor()
    cur.execute("""
        SET search_path TO data_warehouse;
        WITH page_view_agg AS (
            SELECT 
                date_key,
                COUNT(*) AS page_view_count,
                jsonb_agg(jsonb_build_object('page_type', page_type, 'count', cnt)) AS page_view_by_type
            FROM (
                SELECT date_key, page_type, COUNT(*) AS cnt
                FROM fact_page_view
                WHERE date_key = (SELECT MAX(date_key) FROM dim_date)
                GROUP BY date_key, page_type
            ) pv
            GROUP BY date_key
        ),
        product_view_agg AS (
            SELECT 
                date_key,
                COUNT(*) AS product_view_count,
                jsonb_agg(jsonb_build_object('product_sk', product_sk, 'count', cnt)) AS product_view_by_product
            FROM (
                SELECT date_key, product_sk, COUNT(*) AS cnt
                FROM fact_product_view
                WHERE date_key = (SELECT MAX(date_key) FROM dim_date)
                GROUP BY date_key, product_sk
            ) prv
            GROUP BY date_key
        ),
        brand_view_agg AS (
            SELECT 
                date_key,
                COUNT(*) AS brand_view_count,
                jsonb_agg(jsonb_build_object('brand_sk', brand_sk, 'count', cnt)) AS brand_view_by_brand
            FROM (
                SELECT date_key, brand_sk, COUNT(*) AS cnt
                FROM fact_brand_view
                WHERE date_key = (SELECT MAX(date_key) FROM dim_date)
                GROUP BY date_key, brand_sk
            ) bv
            GROUP BY date_key
        ),
        cart_update_agg AS (
            SELECT 
                date_key,
                COUNT(*) AS cart_update_count,
                jsonb_agg(jsonb_build_object('update_type', update_type, 'count', cnt)) AS cart_update_by_type
            FROM (
                SELECT date_key, update_type, COUNT(*) AS cnt
                FROM fact_cart_update
                WHERE date_key = (SELECT MAX(date_key) FROM dim_date)
                GROUP BY date_key, update_type
            ) cu
            GROUP BY date_key
        ),
        user_agg AS (
            SELECT 
                date_key,
                COUNT(*) AS user_count
            FROM fact_page_view
            WHERE date_key = (SELECT MAX(date_key) FROM dim_date)
            GROUP BY date_key
        ),
        purchase_agg AS (
            SELECT 
                date_key,
                COUNT(*) AS purchase_count
            FROM fact_purchase
            WHERE date_key = (SELECT MAX(date_key) FROM dim_date)
            GROUP BY date_key
        ),
        auth_update_agg AS (
            SELECT 
                date_key,
                COUNT(*) AS auth_update_count,
                jsonb_agg(jsonb_build_object('update_type', update_type, 'count', cnt)) AS auth_update_by_type
            FROM (
                SELECT date_key, update_type, COUNT(*) AS cnt
                FROM fact_auth_update
                WHERE date_key = (SELECT MAX(date_key) FROM dim_date)
                GROUP BY date_key, update_type
            ) au
            GROUP BY date_key
        )
        INSERT INTO fact_daily_results (
            date_key,
            page_view_count,
            page_view_by_page,
            product_view_count,
            product_view_by_product,
            brand_view_count,
            brand_view_by_brand,
            cart_update_count,
            cart_update_by_type,
            user_count,
            purchase_count,
            auth_update_count,
            auth_update_by_type
        )
        SELECT 
            d.date_key,
            COALESCE(pv.page_view_count, 0),
            pv.page_view_by_type,
            COALESCE(prv.product_view_count, 0),
            prv.product_view_by_product,
            COALESCE(bv.brand_view_count, 0),
            bv.brand_view_by_brand,
            COALESCE(cu.cart_update_count, 0),
            cu.cart_update_by_type,
            COALESCE(pv.page_view_count, 0),
            COALESCE(p.purchase_count, 0),
            COALESCE(au.auth_update_count, 0),
            au.auth_update_by_type
        FROM dim_date d
        LEFT JOIN page_view_agg pv ON d.date_key = pv.date_key
        LEFT JOIN product_view_agg prv ON d.date_key = prv.date_key
        LEFT JOIN brand_view_agg bv ON d.date_key = bv.date_key
        LEFT JOIN cart_update_agg cu ON d.date_key = cu.date_key
        LEFT JOIN purchase_agg p ON d.date_key = p.date_key
        LEFT JOIN auth_update_agg au ON d.date_key = au.date_key
        WHERE d.date_key = (SELECT MAX(date_key) FROM dim_date);
    """)
    conn.commit()

    return

if __name__ == '__main__':
    batch_result_tables()