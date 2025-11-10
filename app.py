import os
from pathlib import Path
import pandas as pd
import streamlit as st
import duckdb
import shutil

# ─────────────────────────────────────────────────────────
# 경로/설정 (항상 app.py 파일이 있는 폴더 기준으로 고정)
# ─────────────────────────────────────────────────────────
APP_DIR = Path(__file__).parent.resolve()
DB_PATH = APP_DIR / "madang.duckdb"
CSV_CUSTOMER = APP_DIR / "Customer_madang.csv"
CSV_BOOK     = APP_DIR / "Book_madang.csv"
CSV_ORDERS   = APP_DIR / "Orders_madang.csv"


# ─────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────
def _sql_path(p: Path) -> str:
    """DuckDB SQL에 넣을 안전한 경로 문자열 생성 (슬래시/따옴표 이스케이프)."""
    return str(p).replace("\\", "/").replace("'", "''")

def csv_exists_all() -> bool:
    return CSV_CUSTOMER.exists() and CSV_BOOK.exists() and CSV_ORDERS.exists()


# ─────────────────────────────────────────────────────────
# DB 연결(단일 커넥션 캐시)
#  - 잠금 발생 시: 읽기전용 재시도 → 임시복사본 우회 → 실패 시 중단
# ─────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_conn(db_path_str: str):
    # 1차: 일반 모드
    try:
        conn = duckdb.connect(db_path_str)
        conn.execute("PRAGMA enable_progress_bar=false;")
        st.session_state["_readonly"] = False
        return conn
    except duckdb.IOException as e1:
        # 2차: 읽기전용 모드
        try:
            conn = duckdb.connect(db_path_str, read_only=True)
            conn.execute("PRAGMA enable_progress_bar=false;")
            st.session_state["_readonly"] = True
            st.warning("다른 프로세스가 DB를 사용 중이라 **읽기 전용 모드**로 열었습니다.")
            return conn
        except duckdb.IOException as e2:
            # 3차: 임시 복사본으로 시도
            try:
                copy_path = str(Path(db_path_str).with_name(Path(db_path_str).stem + "_temp.duckdb"))
                shutil.copyfile(db_path_str, copy_path)
                conn = duckdb.connect(copy_path)
                conn.execute("PRAGMA enable_progress_bar=false;")
                st.session_state["_readonly"] = False
                st.info(f"원본이 잠겨 있어 **임시 복사본**으로 열었습니다: {copy_path}")
                return conn
            except Exception as e3:
                st.error(
                    "⚠️ DuckDB 파일을 열 수 없습니다.\n\n"
                    "Jupyter/다른 Streamlit/python 프로세스가 DB를 사용 중인지 확인 후 종료하세요.\n\n"
                    f"오류:\n- 일반 모드: {e1}\n- 읽기전용: {e2}\n- 임시복사본: {e3}"
                )
                st.stop()

def clear_conn_cache():
    try:
        get_conn.clear()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────
# 초기화: CSV → DuckDB (필요한 경우만)
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
                "CSV에서 초기화하려 했지만, 필요한 CSV가 없습니다.\n"
                f"- {CSV_CUSTOMER.name}\n- {CSV_BOOK.name}\n- {CSV_ORDERS.name}\n"
                "CSV를 app.py와 같은 폴더(APP_DIR)에 두세요."
            )
            return

        with st.spinner("CSV에서 테이블을 생성 중..."):
            conn.execute("DROP TABLE IF EXISTS Customer;")
            conn.execute("DROP TABLE IF EXISTS Book;")
            conn.execute("DROP TABLE IF EXISTS Orders;")

            c = _sql_path(CSV_CUSTOMER)
            b = _sql_path(CSV_BOOK)
            o = _sql_path(CSV_ORDERS)

            conn.execute(f"CREATE TABLE Customer AS SELECT * FROM read_csv_auto('{c}', HEADER=TRUE);")
            conn.execute(f"CREATE TABLE Book     AS SELECT * FROM read_csv_auto('{b}', HEADER=TRUE);")
            conn.execute(f"CREATE TABLE Orders   AS SELECT * FROM read_csv_auto('{o}', HEADER=TRUE);")

        st.success("CSV로부터 초기화 완료!")


def force_reinit_from_csv(conn: duckdb.DuckDBPyConnection):
    """드롭 후 CSV로 강제 재생성."""
    if not csv_exists_all():
        st.error("CSV 3개가 app.py 폴더에 있어야 합니다.")
        return

    with st.spinner("CSV로 DB를 강제 재초기화 중..."):
        conn.execute("DROP TABLE IF EXISTS Customer;")
        conn.execute("DROP TABLE IF EXISTS Book;")
        conn.execute("DROP TABLE IF EXISTS Orders;")

        c = _sql_path(CSV_CUSTOMER)
        b = _sql_path(CSV_BOOK)
        o = _sql_path(CSV_ORDERS)

        conn.execute(f"CREATE TABLE Customer AS SELECT * FROM read_csv_auto('{c}', HEADER=TRUE);")
        conn.execute(f"CREATE TABLE Book     AS SELECT * FROM read_csv_auto('{b}', HEADER=TRUE);")
        conn.execute(f"CREATE TABLE Orders   AS SELECT * FROM read_csv_auto('{o}', HEADER=TRUE);")

    st.success("재초기화 완료! 테이블이 CSV 내용으로 다시 만들어졌습니다.")


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

    # 사이드바: 상태/도구
    with st.sidebar:
        st.subheader("경로/상태")
        st.write(f"📁 APP_DIR: `{APP_DIR}`")
        st.write(f"🗄️ DB: `{DB_PATH}`")
        st.write(f"📄 CSV 존재: "
                 f"Customer {'✅' if CSV_CUSTOMER.exists() else '❌'}, "
                 f"Book {'✅' if CSV_BOOK.exists() else '❌'}, "
                 f"Orders {'✅' if CSV_ORDERS.exists() else '❌'}")

        if st.button("🔁 DB 재연결 / 캐시 초기화"):
            clear_conn_cache()
            st.rerun()

        st.markdown("---")
        readonly = st.session_state.get("_readonly", False)
        st.caption(("현재 모드: **읽기 전용**" if readonly else "현재 모드: **읽기/쓰기 가능**"))

        if st.button("🧹 CSV로 강제 재초기화(드롭 후 재생성)", disabled=readonly):
            force_reinit_from_csv(get_conn(str(DB_PATH)))
            st.rerun()

        st.markdown("---")
        st.caption(
            "💡 Windows ‘다른 프로세스가 파일 사용 중’ 오류 시: "
            "Jupyter/다른 Streamlit/python 프로세스를 종료하고, "
            "여기서 ‘재연결/캐시 초기화’를 눌러주세요."
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

            readonly = st.session_state.get("_readonly", False)
            if st.button("주문 추가", disabled=readonly):
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
