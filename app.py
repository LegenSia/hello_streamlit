from pathlib import Path
import streamlit as st
import duckdb
import pandas as pd
import os

# ← 여기를 통째로 바꿔 넣기
APP_DIR = Path(__file__).parent.resolve()  # app.py가 있는 폴더
DB_PATH = APP_DIR / "madang.duckdb"       # DB도 app.py 옆에 고정
CSV_CUSTOMER = APP_DIR / "Customer_madang.csv"
CSV_BOOK     = APP_DIR / "Book_madang.csv"
CSV_ORDERS   = APP_DIR / "Orders_madang.csv"


# ─────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────
def csv_exists_all() -> bool:
    return all(Path(name).exists() for name in [CSV_CUSTOMER, CSV_BOOK, CSV_ORDERS])


# ─────────────────────────────────────────────────────────
# DB 연결(단일 커넥션 캐시)
#  - 다른 프로세스가 DB 잡고 있으면 예외 메시지로 안내
# ─────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_conn(db_path_str: str):
    try:
        conn = duckdb.connect(db_path_str)
        conn.execute("PRAGMA enable_progress_bar=false;")
        return conn
    except duckdb.IOException as e:
        st.error(
            "⚠️ DuckDB 파일을 열 수 없습니다.\n\n"
            "다른 파이썬/주피터/스트림릿 프로세스가 같은 DB를 사용 중인지 확인하고 종료하세요.\n\n"
            f"오류 메시지: {e}"
        )
        st.stop()


def clear_conn_cache():
    try:
        get_conn.clear()  # streamlit>=1.25
    except Exception:
        # 일부 버전에서는 clear() 미지원일 수 있음
        pass


# ─────────────────────────────────────────────────────────
# 초기화: CSV → DuckDB (필요한 경우만)
#  - information_schema.tables를 이용해 테이블 유무 확인 (버전 호환)
# ─────────────────────────────────────────────────────────
def init_from_csv_if_needed(conn: duckdb.DuckDBPyConnection):
    tables = set(
        conn.sql("""
            SELECT lower(table_name) AS t
            FROM information_schema.tables
            WHERE table_schema IN ('main','temp','public')
        """).df()["t"].tolist()
    )

    need_create = not {"customer", "book", "orders"}.issubset(tables)

    if need_create:
        if not csv_exists_all():
            st.warning(
                "CSV에서 초기화하려 했지만, 필요한 CSV 파일이 모두 있지 않습니다.\n"
                f"- {CSV_CUSTOMER}\n- {CSV_BOOK}\n- {CSV_ORDERS}\n\n"
                "CSV를 앱 폴더에 두고 다시 시도하세요."
            )
            return

        with st.spinner("CSV에서 테이블을 생성 중..."):
            conn.execute("DROP TABLE IF EXISTS Customer;")
            conn.execute("DROP TABLE IF EXISTS Book;")
            conn.execute("DROP TABLE IF EXISTS Orders;")

            conn.execute(
                "CREATE TABLE Customer AS SELECT * FROM read_csv_auto(?, HEADER=TRUE);",
                [CSV_CUSTOMER],
            )
            conn.execute(
                "CREATE TABLE Book AS SELECT * FROM read_csv_auto(?, HEADER=TRUE);",
                [CSV_BOOK],
            )
            conn.execute(
                "CREATE TABLE Orders AS SELECT * FROM read_csv_auto(?, HEADER=TRUE);",
                [CSV_ORDERS],
            )
            st.success("CSV로부터 초기화 완료!")

    # 필요 시 타입 보정 예시
    # conn.execute("ALTER TABLE Customer ALTER COLUMN custid TYPE INTEGER;")


# ─────────────────────────────────────────────────────────
# 안전 실행 헬퍼
# ─────────────────────────────────────────────────────────
def run_df(conn: duckdb.DuckDBPyConnection, sql: str, params=None) -> pd.DataFrame:
    try:
        if params is None:
            return conn.sql(sql).df()
        return conn.sql(sql, params).df()
    except Exception as e:
        st.error(f"쿼리 실행 오류: {e}")
        return pd.DataFrame()


def run_exec(conn: duckdb.DuckDBPyConnection, sql: str, params=None) -> None:
    try:
        if params is None:
            conn.execute(sql)
        else:
            conn.execute(sql, params)
    except Exception as e:
        st.error(f"쿼리 실행 오류: {e}")


# ─────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────
def main():
    st.set_page_config(page_title="Madang DB (DuckDB + Streamlit)", page_icon="📚", layout="wide")
    st.title("📚 Madang DB — DuckDB + Streamlit")

    # 사이드바: DB 경로 & 재연결
    with st.sidebar:
        st.subheader("DB 설정")
        st.write(f"DB 파일: `{DB_PATH.resolve()}`")

        if st.button("🔁 DB 재연결 / 캐시 초기화"):
            clear_conn_cache()
            st.rerun()

        st.markdown("---")
        st.caption(
            "💡 Windows에서 ‘다른 프로세스가 파일 사용 중’ 오류가 나면\n"
            "작업 관리자에서 해당 python.exe/노트북/스트림릿을 종료하고 다시 시도하세요."
        )

    # 연결
    conn = get_conn(str(DB_PATH))

    # 초기화 (필요 시)
    init_from_csv_if_needed(conn)

    tab1, tab2, tab3, tab4 = st.tabs(["고객 조회", "주문 입력", "테이블 보기", "SQL 콘솔"])

    # ── 고객 조회
    with tab1:
        st.subheader("고객 주문 조회")
        name = st.text_input("고객명", "")
        if name:
            df = run_df(
                conn,
                """
                SELECT c.custid, c.name, b.bookname, o.orderdate, o.saleprice
                FROM Customer c
                JOIN Orders o ON c.custid = o.custid
                JOIN Book b ON o.bookid = b.bookid
                WHERE c.name = ?
                ORDER BY o.orderdate DESC
                """,
                [name],
            )
            if df.empty:
                st.info("해당 고객의 주문이 없습니다.")
            else:
                st.dataframe(df, use_container_width=True)

    # ── 주문 입력
    with tab2:
        st.subheader("새 주문 입력")
        customers = run_df(conn, "SELECT custid, name FROM Customer ORDER BY custid")
        books = run_df(conn, "SELECT bookid, bookname, price FROM Book ORDER BY bookid")

        if not customers.empty and not books.empty:
            cust_display = customers["name"] + " (" + customers["custid"].astype(str) + ")"
            book_display = books["bookname"] + " (" + books["bookid"].astype(str) + ")"

            cust_choice = st.selectbox("고객 선택", cust_display.tolist())
            book_choice = st.selectbox("도서 선택", book_display.tolist())

            default_price = 0
            if "price" in books.columns and not books.empty and pd.notna(books["price"].iloc[0]):
                try:
                    default_price = int(books["price"].iloc[0])
                except Exception:
                    default_price = 0

            saleprice = st.number_input("판매가", min_value=0, step=1000, value=default_price)
            orderdate = st.date_input("주문일", pd.Timestamp.today())

            if st.button("주문 추가"):
                try:
                    custid = int(cust_choice.split("(")[-1].split(")")[0])
                    bookid = int(book_choice.split("(")[-1].split(")")[0])
                except Exception:
                    st.error("고객/도서 선택이 올바르지 않습니다.")
                else:
                    next_id = run_df(conn, "SELECT COALESCE(MAX(orderid), 0) + 1 AS nid FROM Orders")
                    if not next_id.empty:
                        nid = int(next_id["nid"].iloc[0])
                        run_exec(
                            conn,
                            "INSERT INTO Orders VALUES (?, ?, ?, ?, ?)",
                            [nid, custid, bookid, int(saleprice), str(orderdate)],
                        )
                        st.success(f"주문이 추가되었습니다. (orderid={nid})")
        else:
            st.warning("고객 또는 도서 데이터가 비어 있습니다. CSV 초기화가 필요할 수 있어요.")

    # ── 테이블 보기
    with tab3:
        st.subheader("테이블 브라우저")
        table = st.selectbox("테이블 선택", ["Customer", "Book", "Orders"])
        df = run_df(conn, f"SELECT * FROM {table}")
        st.dataframe(df, use_container_width=True)

    # ── SQL 콘솔 (고급)
    with tab4:
        st.subheader("SQL 콘솔 (고급 사용자용)")
        st.caption("SELECT, INSERT 등 자유롭게 실행 (주의: 데이터 변경 가능)")
        sql = st.text_area("SQL 입력", "SELECT * FROM Customer LIMIT 10;")
        if st.button("실행"):
            try:
                res = conn.sql(sql)
                try:
                    df = res.df()
                    st.dataframe(df, use_container_width=True)
                except Exception:
                    st.success("쿼리가 실행되었습니다.")
            except Exception as e:
                st.error(f"실행 오류: {e}")


if __name__ == "__main__":
    main()
