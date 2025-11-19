import streamlit as st
import sqlite3
from contextlib import closing
from datetime import datetime, date, timedelta
import pandas as pd
from streamlit_calendar import calendar as st_calendar  # pip install streamlit-calendar
from st_circular_progress import CircularProgress       # pip install st-circular-progress

DB = "collab.db"


# ====================================
# DB Helpers & 초기화
# ====================================
def get_conn():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(get_conn()) as conn, conn:
        conn.executescript(
            """
        CREATE TABLE IF NOT EXISTS projects(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS parts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            color TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            part_id INTEGER,
            role TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(part_id) REFERENCES parts(id)
        );

        CREATE TABLE IF NOT EXISTS tasks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            part_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            assignee TEXT,
            priority TEXT CHECK(priority IN ('Low','Medium','High')) DEFAULT 'Medium',
            status TEXT CHECK(status IN ('Todo','In Progress','Done')) DEFAULT 'Todo',
            start_date TEXT,
            due_date TEXT,
            progress INTEGER CHECK(progress BETWEEN 0 AND 100) DEFAULT 0,
            tags TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(project_id) REFERENCES projects(id),
            FOREIGN KEY(part_id) REFERENCES parts(id)
        );

        CREATE TABLE IF NOT EXISTS user_parts(
            user_id INTEGER NOT NULL,
            part_id INTEGER NOT NULL,
            PRIMARY KEY(user_id, part_id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(part_id) REFERENCES parts(id)
        );

        CREATE TABLE IF NOT EXISTS user_projects(
            user_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            PRIMARY KEY(user_id, project_id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(project_id) REFERENCES projects(id)
        );
        """
        )

        # parts에 color 컬럼 없으면 추가
        cur = conn.execute("PRAGMA table_info(parts)")
        cols = [r["name"] for r in cur.fetchall()]
        if "color" not in cols:
            conn.execute("ALTER TABLE parts ADD COLUMN color TEXT")


def seed_if_empty():
    now = datetime.utcnow().isoformat()
    with closing(get_conn()) as conn, conn:
        # '데모 프로젝트' 제거
        conn.execute(
            "DELETE FROM tasks WHERE project_id IN (SELECT id FROM projects WHERE name='데모 프로젝트')"
        )
        conn.execute("DELETE FROM projects WHERE name='데모 프로젝트'")

        # '빈 샘플 프로젝트' 보장
        c = conn.execute(
            "SELECT COUNT(*) AS c FROM projects WHERE name='빈 샘플 프로젝트'"
        ).fetchone()["c"]
        if c == 0:
            conn.execute(
                "INSERT INTO projects(name, description, created_at) VALUES(?,?,?)",
                ("빈 샘플 프로젝트", "빈 프로젝트 (테스트용)", now),
            )

        # 파트 + 색상
        default_colors = {
            "기획": "#F97373",
            "개발": "#6CB2EB",
            "아트": "#FBC15E",
        }
        existing = conn.execute("SELECT id, name, color FROM parts").fetchall()
        existing_names = {r["name"] for r in existing}

        for name, color in default_colors.items():
            if name not in existing_names:
                conn.execute(
                    "INSERT INTO parts(name, color, created_at) VALUES(?,?,?)",
                    (name, color, now),
                )

        rows = conn.execute("SELECT id, name, color FROM parts").fetchall()
        for r in rows:
            if r["color"] is None:
                color = default_colors.get(r["name"], "#3788d8")
                conn.execute(
                    "UPDATE parts SET color=? WHERE id=?", (color, r["id"])
                )

        # 유저
        c_users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        if c_users == 0:
            parts_map = {
                row["name"]: row["id"]
                for row in conn.execute("SELECT id,name FROM parts").fetchall()
            }
            sample_users = [
                ("기획자 A", "planner@example.com", parts_map.get("기획"), "planner"),
                ("개발자 B", "dev@example.com", parts_map.get("개발"), "developer"),
                ("아티스트 C", "artist@example.com", parts_map.get("아트"), "artist"),
            ]
            for u in sample_users:
                conn.execute(
                    "INSERT INTO users(name,email,part_id,role,created_at) VALUES(?,?,?,?,?)",
                    (*u, now),
                )

        # users.part_id → user_parts
        rows = conn.execute(
            "SELECT id, part_id FROM users WHERE part_id IS NOT NULL"
        ).fetchall()
        for r in rows:
            conn.execute(
                "INSERT OR IGNORE INTO user_parts(user_id, part_id) VALUES(?,?)",
                (r["id"], r["part_id"]),
            )

        # user_projects: 기본 전체 프로젝트 권한
        users = conn.execute("SELECT id FROM users").fetchall()
        projects = conn.execute("SELECT id FROM projects").fetchall()
        for u in users:
            for p in projects:
                conn.execute(
                    "INSERT OR IGNORE INTO user_projects(user_id, project_id) VALUES(?,?)",
                    (u["id"], p["id"]),
                )

        # 샘플 작업
        c_tasks = conn.execute("SELECT COUNT(*) AS c FROM tasks").fetchone()["c"]
        if c_tasks == 0:
            proj_row = conn.execute(
                "SELECT id FROM projects ORDER BY id LIMIT 1"
            ).fetchone()
            if proj_row:
                project_id = proj_row["id"]
                parts_map = {
                    row["name"]: row["id"]
                    for row in conn.execute("SELECT id,name FROM parts").fetchall()
                }
                sample_tasks = [
                    (
                        project_id,
                        parts_map["기획"],
                        "기획 문서 정리",
                        "요구사항 수집|40|0\n와이어프레임 정리|60|0",
                        "기획자 A",
                        "High",
                        "Todo",
                        (date.today() - timedelta(days=1)).isoformat(),
                        (date.today() + timedelta(days=2)).isoformat(),
                        0,
                        "기획,문서",
                    ),
                ]
                for t in sample_tasks:
                    conn.execute(
                        """
                    INSERT INTO tasks(
                        project_id, part_id, title, description, assignee, priority, status,
                        start_date, due_date, progress, tags, created_at, updated_at
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                        (*t, now, now),
                    )


# ---------- Data access ----------
def list_projects():
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            "SELECT * FROM projects ORDER BY created_at DESC, id DESC", conn
        )


def list_parts():
    with closing(get_conn()) as conn:
        return pd.read_sql_query("SELECT * FROM parts ORDER BY id", conn)


def list_users():
    with closing(get_conn()) as conn:
        query = """
        SELECT u.*,
               GROUP_CONCAT(p.name, ', ') AS part_names
        FROM users u
        LEFT JOIN user_parts up ON up.user_id = u.id
        LEFT JOIN parts p ON p.id = up.part_id
        GROUP BY u.id
        ORDER BY u.id
        """
        return pd.read_sql_query(query, conn)


def list_tasks(project_id=None, part_id=None):
    with closing(get_conn()) as conn:
        base = """
        SELECT t.*, p.name AS part_name, p.color AS part_color
        FROM tasks t
        JOIN parts p ON p.id = t.part_id
        """
        conds = []
        params = []
        if project_id is not None:
            conds.append("t.project_id = ?")
            params.append(project_id)
        if part_id is not None:
            conds.append("t.part_id = ?")
            params.append(part_id)
        if conds:
            base += " WHERE " + " AND ".join(conds)
        base += " ORDER BY t.due_date IS NULL, t.due_date ASC, t.id DESC"
        return pd.read_sql_query(base, conn, params=params)


def insert_project(name, description):
    now = datetime.utcnow().isoformat()
    with closing(get_conn()) as conn, conn:
        conn.execute(
            "INSERT INTO projects(name, description, created_at) VALUES(?,?,?)",
            (name, description, now),
        )


def update_project(project_id, **kwargs):
    sets = []
    params = []
    for k, v in kwargs.items():
        sets.append(f"{k}=?")
        params.append(v)
    params.append(project_id)
    with closing(get_conn()) as conn, conn:
        if sets:
            conn.execute(
                f"UPDATE projects SET {', '.join(sets)} WHERE id=?", params
            )


def insert_part(name, color="#3788d8"):
    now = datetime.utcnow().isoformat()
    with closing(get_conn()) as conn, conn:
        conn.execute(
            "INSERT INTO parts(name, color, created_at) VALUES(?,?,?)",
            (name, color, now),
        )


def update_part(part_id, **kwargs):
    sets = []
    params = []
    for k, v in kwargs.items():
        sets.append(f"{k}=?")
        params.append(v)
    params.append(part_id)
    with closing(get_conn()) as conn, conn:
        if sets:
            conn.execute(
                f"UPDATE parts SET {', '.join(sets)} WHERE id=?", params
            )


def insert_user(name, email, part_ids, role):
    now = datetime.utcnow().isoformat()
    main_part_id = part_ids[0] if part_ids else None
    with closing(get_conn()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO users(name,email,part_id,role,created_at) VALUES(?,?,?,?,?)",
            (name, email, main_part_id, role, now),
        )
        user_id = cur.lastrowid
        for pid in part_ids or []:
            conn.execute(
                "INSERT OR IGNORE INTO user_parts(user_id, part_id) VALUES(?,?)",
                (user_id, pid),
            )
        prows = conn.execute("SELECT id FROM projects").fetchall()
        for p in prows:
            conn.execute(
                "INSERT OR IGNORE INTO user_projects(user_id, project_id) VALUES(?,?)",
                (user_id, p["id"]),
            )
        return user_id


def update_user(user_id, **kwargs):
    sets = []
    params = []
    for k, v in kwargs.items():
        sets.append(f"{k}=?")
        params.append(v)
    params.append(user_id)
    with closing(get_conn()) as conn, conn:
        if sets:
            conn.execute(
                f"UPDATE users SET {', '.join(sets)} WHERE id=?", params
            )


def delete_user(user_id):
    with closing(get_conn()) as conn, conn:
        conn.execute("DELETE FROM user_parts WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM user_projects WHERE user_id=?", (user_id,))
        conn.execute("DELETE FROM users WHERE id=?", (user_id,))


def set_user_parts(user_id, part_ids):
    with closing(get_conn()) as conn, conn:
        conn.execute("DELETE FROM user_parts WHERE user_id=?", (user_id,))
        for pid in part_ids or []:
            conn.execute(
                "INSERT OR IGNORE INTO user_parts(user_id, part_id) VALUES(?,?)",
                (user_id, pid),
            )


def get_parts_for_user(user_id):
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
        SELECT p.*
        FROM user_parts up
        JOIN parts p ON p.id = up.part_id
        WHERE up.user_id = ?
        ORDER BY p.id
        """,
            conn,
            params=(user_id,),
        )


def get_parts_for_user_name(user_name):
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
        SELECT p.*
        FROM users u
        JOIN user_parts up ON u.id = up.user_id
        JOIN parts p ON p.id = up.part_id
        WHERE u.name = ?
        ORDER BY p.id
        """,
            conn,
            params=(user_name,),
        )


def get_users_for_part(part_id):
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
        SELECT u.*
        FROM users u
        JOIN user_parts up ON up.user_id = u.id
        WHERE up.part_id = ?
        ORDER BY u.id
        """,
            conn,
            params=(part_id,),
        )


def get_projects_for_user(user_id):
    with closing(get_conn()) as conn:
        return pd.read_sql_query(
            """
        SELECT pr.*
        FROM user_projects up
        JOIN projects pr ON pr.id = up.project_id
        WHERE up.user_id = ?
        ORDER BY pr.created_at DESC, pr.id DESC
        """,
            conn,
            params=(user_id,),
        )


def set_user_projects(user_id, project_ids):
    with closing(get_conn()) as conn, conn:
        conn.execute("DELETE FROM user_projects WHERE user_id=?", (user_id,))
        for pid in project_ids or []:
            conn.execute(
                "INSERT OR IGNORE INTO user_projects(user_id, project_id) VALUES(?,?)",
                (user_id, pid),
            )


def insert_task(
    project_id,
    part_id,
    title,
    description,
    assignee,
    priority,
    status,
    start_date,
    due_date,
    progress,
    tags,
):
    now = datetime.utcnow().isoformat()
    with closing(get_conn()) as conn, conn:
        conn.execute(
            """
            INSERT INTO tasks(
                project_id, part_id, title, description, assignee,
                priority, status, start_date, due_date, progress, tags,
                created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                project_id,
                part_id,
                title,
                description,
                assignee,
                priority,
                status,
                start_date,
                due_date,
                progress,
                tags,
                now,
                now,
            ),
        )


def update_task(task_id, **kwargs):
    now = datetime.utcnow().isoformat()
    sets = []
    params = []
    for k, v in kwargs.items():
        sets.append(f"{k}=?")
        params.append(v)
    sets.append("updated_at=?")
    params.append(now)
    params.append(task_id)
    with closing(get_conn()) as conn, conn:
        conn.execute(
            f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", params
        )


def delete_task(task_id):
    with closing(get_conn()) as conn, conn:
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))


# ---------- 색 변형 (같은 파트 안에서 살짝씩 다르게) ----------
def adjust_color(hex_color: str, index: int) -> str:
    if not isinstance(hex_color, str) or not hex_color:
        hex_color = "#3788d8"
    c = hex_color.lstrip("#")
    if len(c) != 6:
        c = "3788d8"
    r = int(c[0:2], 16)
    g = int(c[2:4], 16)
    b = int(c[4:6], 16)
    offsets = [-0.4, -0.2, 0.0, 0.2, 0.4]
    factor = 1.0 + offsets[index % len(offsets)]
    r = max(0, min(255, int(r * factor)))
    g = max(0, min(255, int(g * factor)))
    b = max(0, min(255, int(b * factor)))
    return f"#{r:02X}{g:02X}{b:02X}"


# ---------- Calendar helper ----------
def build_calendar_events(tasks_df, show_part_in_title=True):
    events = []
    if tasks_df is None or tasks_df.empty:
        return events

    color_idx_by_part = {}

    for _, r in tasks_df.iterrows():
        s = None
        e = None
        if isinstance(r.get("start_date"), str) and r["start_date"]:
            s = r["start_date"]
        if isinstance(r.get("due_date"), str) and r["due_date"]:
            e = r["due_date"]
        if not s and e:
            s = e
        if not e and s:
            e = s
        if not s and not e:
            s = e = date.today().isoformat()

        title = r["title"]
        if show_part_in_title and isinstance(r.get("part_name"), str):
            title = f"[{r['part_name']}] {title}"

        base_color = (
            r.get("part_color")
            if isinstance(r.get("part_color"), str) and r["part_color"]
            else "#3788d8"
        )

        part_id = r.get("part_id")
        idx = color_idx_by_part.get(part_id, 0)
        color_idx_by_part[part_id] = idx + 1
        color = adjust_color(base_color, idx)

        event = {
            "id": str(r["id"]),
            "title": title,
            "start": s,
            "end": e,
            "allDay": True,
            "backgroundColor": color,
            "borderColor": color,
            "extendedProps": {
                "assignee": r.get("assignee"),
                "priority": r.get("priority"),
                "status": r.get("status"),
            },
        }
        events.append(event)
    return events


def calendar_options_base():
    return {
        "initialView": "dayGridMonth",
        "headerToolbar": {
            "left": "title",
            "center": "",
            "right": "dayGridMonth,dayGridWeek,dayGridDay prev,next",
        },
        "locale": "ko",
        "selectable": True,
        "editable": False,
        "height": 550,
        "contentHeight": 480,
        "aspectRatio": 1.35,
    }


# ---------- Description <-> subtasks ----------
# 포맷: label|weight|done(0/1)
def parse_subtasks(description: str):
    if not description:
        return []
    lines = description.splitlines()
    result = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if "|" in line:
            parts = [p.strip() for p in line.split("|")]
            label = parts[0]
            weight = 0
            done = False
            if len(parts) > 1:
                try:
                    weight = int(parts[1])
                except Exception:
                    weight = 0
            if len(parts) > 2:
                try:
                    done = bool(int(parts[2]))
                except Exception:
                    done = False
            result.append((label, max(0, min(100, weight)), done))
            continue

        done = False
        label = line
        if line.startswith("[x] "):
            done = True
            label = line[4:]
        elif line.startswith("[ ] "):
            done = False
            label = line[4:]
        weight = 100
        result.append((label.strip(), weight, done))
    return result


def serialize_subtasks(subtasks):
    lines = []
    for label, weight, done in subtasks:
        lines.append(f"{label}|{int(weight)}|{1 if done else 0}")
    return "\n".join(lines)


def calc_progress_from_subtasks(subtasks):
    if not subtasks:
        return 0
    s = sum(int(w) for _, w, done in subtasks if done)
    return min(100, max(0, s))


def priority_label_and_color(priority: str):
    if priority == "High":
        return "높음", "#FF4B4B"
    if priority == "Low":
        return "낮음", "#4CAF50"
    return "중간", "#FFDD57"


def completion_ratio(tasks_df: pd.DataFrame) -> int:
    if tasks_df is None or tasks_df.empty:
        return 0
    total = len(tasks_df)
    done_equiv = 0.0
    for _, r in tasks_df.iterrows():
        status = r.get("status") or ""
        prog = r.get("progress") or 0
        if status == "Done":
            done_equiv += 1.0
        elif status == "In Progress":
            try:
                done_equiv += float(prog) / 100.0
            except Exception:
                pass
    return int(round(100 * done_equiv / total))


# ====================================
# Streamlit UI & 로그인
# ====================================
st.set_page_config(page_title="협업툴 - 일정/진행도", layout="wide")

init_db()
seed_if_empty()

st.markdown(
    """
<style>
.red-button button {
    background-color: #ff4b4b !important;
    color: white !important;
}
</style>
""",
    unsafe_allow_html=True,
)

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
    st.session_state["role"] = None
    st.session_state["current_tab"] = "대시보드"

# ---- 로그인 화면 ----
if not st.session_state["logged_in"]:
    st.title("협업툴 로그인")

    with st.form("login_form"):
        company = st.selectbox("회사", ["Inha"], index=0)
        username = st.text_input("아이디")
        password = st.text_input("비밀번호", type="password")
        login_btn = st.form_submit_button("로그인")

        if login_btn:
            if (
                company == "Inha"
                and password == "1234"
                and username in ["admin", "user"]
            ):
                st.session_state["logged_in"] = True
                st.session_state["role"] = username  # "admin" or "user"
                st.session_state["current_tab"] = "대시보드"
                st.rerun()
            else:
                st.error("로그인 정보가 올바르지 않습니다.")
    st.stop()

# 로그인 이후
CURRENT_USER = "기획자 A"  # 데모용 고정

projects_df = list_projects()
parts_df = list_parts()
part_names = parts_df["name"].tolist()
users_df = list_users()

if "current_tab" not in st.session_state:
    st.session_state["current_tab"] = "대시보드"

# user 계정이 관리자 탭에 들어가 있었으면 강제로 대시보드로
if st.session_state["role"] == "user" and st.session_state["current_tab"] in [
    "프로젝트 관리",
    "유저 관리",
]:
    st.session_state["current_tab"] = "대시보드"

# -------- 사이드바 --------
with st.sidebar:
    st.markdown("### 프로젝트")
    if projects_df.empty:
        selected_project_id = None
        selected_project_name = ""
        st.selectbox("", ["프로젝트 없음"], disabled=True)
    else:
        project_names = projects_df["name"].tolist()
        selected_project_name = st.selectbox("", project_names)
        selected_project_id = int(
            projects_df[projects_df["name"] == selected_project_name]["id"].iloc[0]
        )

    st.write("")
    if st.button("대시보드", use_container_width=True):
        st.session_state["current_tab"] = "대시보드"

    st.markdown("---")
    st.write("### 파트")

    for pname in part_names:
        if st.button(pname, use_container_width=True, key=f"tab_{pname}"):
            st.session_state["current_tab"] = f"파트:{pname}"

    # 관리자 메뉴는 admin에게만
    if st.session_state["role"] == "admin":
        st.markdown("---")
        st.markdown("### 관리자")
        if st.button("프로젝트 관리", use_container_width=True):
            st.session_state["current_tab"] = "프로젝트 관리"
        if st.button("유저 관리", use_container_width=True):
            st.session_state["current_tab"] = "유저 관리"

    # 맨 하단 로그아웃 버튼
    st.markdown("---")
    if st.button("로그아웃", use_container_width=True):
        st.session_state["logged_in"] = False
        st.session_state["role"] = None
        st.session_state["current_tab"] = "대시보드"
        st.rerun()

current_tab = st.session_state["current_tab"]

if selected_project_id:
    st.title(selected_project_name)
else:
    st.title("프로젝트가 없습니다")

# ====================================
# 대시보드
# ====================================
if current_tab == "대시보드":
    st.subheader("📊 대시보드 (전체 파트 일정)")

    if not selected_project_id:
        st.info("좌측에서 프로젝트를 선택하세요.")
    else:
        col1, col2 = st.columns([3, 1])
        with col2:
            part_filter_name = st.selectbox("파트 필터", ["전체"] + part_names)
        with col1:
            pass

        all_tasks = list_tasks(project_id=selected_project_id)

        if part_filter_name != "전체":
            part_row = parts_df[parts_df["name"] == part_filter_name]
            if not part_row.empty:
                part_id_filter = int(part_row["id"].iloc[0])
                filtered = list_tasks(
                    project_id=selected_project_id, part_id=part_id_filter
                )
            else:
                filtered = all_tasks.iloc[0:0]
        else:
            filtered = all_tasks

        events = build_calendar_events(filtered, show_part_in_title=True)
        options = calendar_options_base()
        cal_val = st_calendar(
            events=events,
            options=options,
            key="dashboard_calendar",
        )

        key_sel = "dashboard_selected_date"
        default_sel = st.session_state.get(key_sel, date.today().isoformat())
        if isinstance(cal_val, dict) and cal_val.get("callback") == "dateClick":
            d_str = cal_val["dateClick"]["date"][:10]
            st.session_state[key_sel] = d_str
            default_sel = d_str
        selected_day = date.fromisoformat(default_sel)

        def is_on_day(row):
            due = row.get("due_date")
            if not isinstance(due, str) or not due:
                return False
            try:
                d = date.fromisoformat(due)
                return d == selected_day
            except Exception:
                return False

        day_tasks = (
            filtered[filtered.apply(is_on_day, axis=1)]
            if not filtered.empty
            else filtered
        )

        if not day_tasks.empty:
            st.markdown("#### 선택한 날짜 일정")
            show_cols = [
                "title",
                "part_name",
                "assignee",
                "status",
                "priority",
                "start_date",
                "due_date",
                "progress",
                "tags",
            ]
            exist_cols = [c for c in show_cols if c in day_tasks.columns]
            st.dataframe(
                day_tasks[exist_cols], use_container_width=True, hide_index=True
            )

        br_col, graph_col = st.columns([2, 2])

        with br_col:
            st.markdown("#### 🧍 나의 할 일 브리핑 (기획자 A 기준)")
            if filtered.empty:
                st.caption("현재 프로젝트에 등록된 작업이 없습니다.")
            else:
                my_tasks = filtered[filtered["assignee"] == CURRENT_USER]
                if my_tasks.empty:
                    st.caption(
                        "현재 프로젝트/필터에서 기획자 A에게 배정된 작업이 없습니다."
                    )
                else:
                    total = len(my_tasks)
                    by_status = my_tasks["status"].value_counts().to_dict()

                    def parse_due(x):
                        try:
                            return (
                                date.fromisoformat(x)
                                if isinstance(x, str) and x
                                else None
                            )
                        except Exception:
                            return None

                    my_tasks["due_dt"] = my_tasks["due_date"].apply(parse_due)
                    upcoming = my_tasks.dropna(subset=["due_dt"]).sort_values("due_dt")
                    if not upcoming.empty:
                        next_due = upcoming.iloc[0]
                        next_due_date = next_due["due_dt"].isoformat()
                        next_due_title = next_due["title"]
                    else:
                        next_due_date = "-"
                        next_due_title = "-"

                    st.markdown(
                        f"- 총 작업 수: **{total}건**  "
                        f"(Todo: {by_status.get('Todo', 0)}, In Progress: {by_status.get('In Progress', 0)}, Done: {by_status.get('Done', 0)})"
                    )
                    st.markdown(
                        f"- 가장 가까운 마감: **{next_due_date} · {next_due_title}**"
                    )

        with graph_col:
            st.markdown("#### 전체 / 파트 진행률")
            if all_tasks is None or all_tasks.empty:
                st.caption("진행률 데이터가 없습니다.")
            else:
                overall = completion_ratio(all_tasks)
                items = []

                items.append(
                    {
                        "label": "전체",
                        "value": overall,
                        "color": "#4A5568",
                    }
                )

                for _, prow in parts_df.iterrows():
                    pid = prow["id"]
                    pname = prow["name"]
                    pcolor = (
                        prow["color"]
                        if isinstance(prow["color"], str) and prow["color"]
                        else "#3788d8"
                    )
                    ptasks = all_tasks[all_tasks["part_id"] == pid]
                    val = completion_ratio(ptasks) if not ptasks.empty else 0
                    items.append(
                        {
                            "label": pname,
                            "value": val,
                            "color": pcolor,
                        }
                    )

                n_items = len(items)
                max_cols = 4
                idx = 0
                while idx < n_items:
                    cols = st.columns(min(max_cols, n_items - idx))
                    for c in range(len(cols)):
                        item = items[idx]
                        with cols[c]:
                            CircularProgress(
                                label=item["label"],
                                value=item["value"],
                                key=f"cp_{item['label']}_{idx}",
                                color=item["color"],
                            ).st_circular_progress()
                        idx += 1

# ====================================
# 프로젝트 관리 탭 (admin 전용)
# ====================================
elif current_tab == "프로젝트 관리" and st.session_state["role"] == "admin":
    st.subheader("🧩 프로젝트 관리")

    top_left, top_right = st.columns(2)

    with top_left:
        st.markdown("#### 프로젝트 목록")
        projects_df = list_projects()
        if projects_df.empty:
            st.caption("등록된 프로젝트가 없습니다.")
        else:
            st.dataframe(
                projects_df[["id", "name", "description", "created_at"]],
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("#### 프로젝트 이름 수정")
        projects_df = list_projects()
        if not projects_df.empty:
            proj_labels = [
                f"{r['name']} (id={r['id']})" for _, r in projects_df.iterrows()
            ]
            sel_label = st.selectbox(
                "수정할 프로젝트 선택", proj_labels, key="edit_proj_sel"
            )
            idx = proj_labels.index(sel_label)
            row = projects_df.iloc[idx]
            new_name = st.text_input(
                "새 이름", value=row["name"], key="edit_proj_name"
            )
            new_desc = st.text_input(
                "새 설명", value=row.get("description") or "", key="edit_proj_desc"
            )
            if st.button("프로젝트 수정"):
                update_project(
                    int(row["id"]),
                    name=new_name.strip() or row["name"],
                    description=new_desc.strip(),
                )
                st.success("프로젝트가 수정되었습니다.")

    with top_right:
        st.markdown("#### 파트 목록 / 수정")
        parts_df = list_parts()
        if parts_df.empty:
            st.caption("등록된 파트가 없습니다.")
        else:
            for _, row in parts_df.iterrows():
                c1, c2, c3 = st.columns([3, 2, 1])
                with c1:
                    new_part_name = st.text_input(
                        "이름",
                        value=row["name"],
                        key=f"part_name_{row['id']}",
                        label_visibility="collapsed",
                    )
                with c2:
                    current_color = (
                        row["color"]
                        if isinstance(row["color"], str) and row["color"]
                        else "#3788d8"
                    )
                    color_val = st.color_picker(
                        "색상",
                        current_color,
                        key=f"part_color_{row['id']}",
                        label_visibility="collapsed",
                    )
                with c3:
                    if st.button("저장", key=f"save_part_{row['id']}"):
                        update_part(
                            row["id"],
                            name=new_part_name.strip() or row["name"],
                            color=color_val,
                        )
                        st.success(f"{row['name']} 파트가 업데이트되었습니다.")

    st.markdown("---")

    bottom_left, bottom_right = st.columns(2)

    with bottom_left:
        st.markdown("#### 프로젝트 추가")
        with st.form("add_project"):
            p_name = st.text_input("프로젝트 이름*", key="new_proj_name")
            p_desc = st.text_input("설명", key="new_proj_desc")
            add_proj = st.form_submit_button("추가")
            if add_proj:
                if not p_name.strip():
                    st.error("프로젝트 이름은 필수입니다.")
                else:
                    insert_project(p_name.strip(), p_desc.strip())
                    st.success("프로젝트가 추가되었습니다.")

    with bottom_right:
        st.markdown("#### 파트 추가")
        with st.form("add_part"):
            new_part_name = st.text_input(
                "새 파트 이름", placeholder="예: QA, 운영 등", key="new_part_name"
            )
            new_part_color = st.color_picker(
                "색상", "#3788d8", key="new_part_color"
            )
            submitted = st.form_submit_button("추가")
            if submitted:
                parts_df = list_parts()
                if not new_part_name.strip():
                    st.error("파트명을 입력하세요.")
                elif new_part_name.strip() in parts_df["name"].tolist():
                    st.error("이미 존재하는 파트입니다.")
                else:
                    insert_part(new_part_name.strip(), new_part_color)
                    st.success("파트가 추가되었습니다.")

# ====================================
# 유저 관리 탭 (admin 전용)
# ====================================
elif current_tab == "유저 관리" and st.session_state["role"] == "admin":
    st.subheader("👤 유저 관리")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 유저 목록")
        users_df = list_users()
        if users_df.empty:
            st.info("등록된 유저가 없습니다.")
        else:
            show_cols = ["name", "email", "part_names", "role"]
            exist_cols = [c for c in show_cols if c in users_df.columns]
            st.dataframe(
                users_df[exist_cols], use_container_width=True, hide_index=True
            )

    with col2:
        # --- 유저 추가 (박스 없이 제목만) ---
        st.markdown("#### 유저 추가")
        with st.form("add_user"):
            u_name = st.text_input("이름*")
            u_email = st.text_input("이메일")
            u_role = st.text_input("역할", placeholder="예: planner, dev 등")
            parts_selected = st.multiselect("파트(여러 개 선택 가능)", part_names)
            submitted = st.form_submit_button("유저 생성")
            if submitted:
                if not u_name.strip():
                    st.error("이름은 필수입니다.")
                else:
                    part_ids = []
                    parts_df = list_parts()
                    for pn in parts_selected:
                        pid = int(
                            parts_df[parts_df["name"] == pn]["id"].iloc[0]
                        )
                        part_ids.append(pid)
                    insert_user(
                        u_name.strip(),
                        u_email.strip() or None,
                        part_ids,
                        u_role.strip() or None,
                    )
                    st.success("유저가 추가되었습니다.")

        st.write("")
        users_df = list_users()
        if users_df.empty:
            st.info("유저가 없습니다.")
        else:
            # --- 유저 상세 설정 제목은 박스 밖으로 ---
            st.markdown("#### 유저 상세 설정")

            # 전체 영역은 박스로 감싸기
            with st.container(border=True):
                # 유저 선택
                user_labels = [
                    f"{r['name']} ({r['email'] or '-'})"
                    for _, r in users_df.iterrows()
                ]
                selected_label = st.selectbox(
                    "유저 선택",
                    user_labels,
                    key="user_select",
                )
                idx = user_labels.index(selected_label)
                user_row = users_df.iloc[idx]
                user_id = int(user_row["id"])

                parts_df = list_parts()
                projects_df = list_projects()
                proj_names = projects_df["name"].tolist()
                proj_id_by_name = {
                    r["name"]: r["id"] for _, r in projects_df.iterrows()
                }

                # 현재 파트 / 프로젝트
                user_parts_df = get_parts_for_user(user_id)
                current_part_names = (
                    user_parts_df["name"].tolist()
                    if not user_parts_df.empty
                    else []
                )
                user_proj_df = get_projects_for_user(user_id)
                current_proj_ids = (
                    user_proj_df["id"].tolist()
                    if not user_proj_df.empty
                    else []
                )
                current_proj_names = [
                    name
                    for name in proj_names
                    if proj_id_by_name[name] in current_proj_ids
                ]

                # 파트 / 프로젝트 선택
                new_parts = st.multiselect(
                    "파트",
                    part_names,
                    default=current_part_names,
                )
                new_proj_names = st.multiselect(
                    "접속 가능한 프로젝트",
                    proj_names,
                    default=current_proj_names,
                )

                # 버튼: [저장 및 수정] [유저 삭제]  (새 작업 추가의 버튼 배열처럼)
                btn_col1, btn_col2 = st.columns(2, gap="small")
                with btn_col1:
                    if st.button(
                        "저장 및 수정",
                        key="save_user_parts",
                        use_container_width=True,
                    ):
                        # 파트 저장
                        new_part_ids = []
                        for pn in new_parts:
                            pid = int(
                                parts_df[parts_df["name"] == pn]["id"].iloc[0]
                            )
                            new_part_ids.append(pid)
                        set_user_parts(user_id, new_part_ids)
                        main_part_id = new_part_ids[0] if new_part_ids else None
                        update_user(user_id, part_id=main_part_id)

                        # 프로젝트 저장
                        new_proj_ids = [proj_id_by_name[n] for n in new_proj_names]
                        set_user_projects(user_id, new_proj_ids)
                        st.success("설정이 저장·수정되었습니다.")
                        st.rerun()

                with btn_col2:
                    del_clicked = st.button("유저 삭제", key=f"del_user_{user_id}", use_container_width=True, type="secondary")
                    
                    if del_clicked:
                        st.session_state["confirm_del_user"] = user_id

        # 삭제 확인 (박스 밖에 위치)
        if (
            "confirm_del_user" in st.session_state
            and not users_df.empty
        ):
            cid = st.session_state.get("confirm_del_user")
            if cid is not None:
                st.warning("정말 삭제할까요? 아래 버튼을 누르면 삭제됩니다.")
                c1, c2 = st.columns([1, 1])
                with c1:
                    if st.button(
                        "네, 삭제합니다",
                        key=f"confirm_del_user_btn_{cid}",
                        use_container_width=True,
                    ):
                        delete_user(cid)
                        st.session_state.pop("confirm_del_user", None)
                        st.warning("유저가 삭제되었습니다.")
                        st.rerun()
                with c2:
                    if st.button(
                        "취소",
                        key=f"cancel_del_user_{cid}",
                        use_container_width=True,
                    ):
                        st.session_state.pop("confirm_del_user", None)


# ====================================
# 파트별 화면
# ====================================
else:
    if current_tab.startswith("파트:"):
        part_name = current_tab.split("파트:", 1)[1]
    else:
        part_name = current_tab

    st.subheader(f"🗂 {part_name} 파트 작업 보드")

    if not selected_project_id:
        st.info("좌측에서 프로젝트를 선택하세요.")
    else:
        parts_df = list_parts()
        part_row = parts_df[parts_df["name"] == part_name]
        if part_row.empty:
            st.error("해당 파트 정보를 찾을 수 없습니다.")
        else:
            part_id = int(part_row["id"].iloc[0])
            tdf = list_tasks(project_id=selected_project_id, part_id=part_id)

            events = build_calendar_events(tdf, show_part_in_title=False)
            options = calendar_options_base()
            cal_val = st_calendar(
                events=events,
                options=options,
                key=f"calendar_part_{part_id}",
            )

            key_sel = f"part_{part_id}_selected_date"
            default_sel = st.session_state.get(key_sel, date.today().isoformat())
            if isinstance(cal_val, dict) and cal_val.get("callback") == "dateClick":
                d_str = cal_val["dateClick"]["date"][:10]
                st.session_state[key_sel] = d_str
                default_sel = d_str
            selected_day = date.fromisoformat(default_sel)

            with st.expander("🔍 필터", expanded=False):
                f1, f2, f3, f4 = st.columns(4)
                with f1:
                    assignee_filter = st.text_input("담당자(부분일치)")
                with f2:
                    status_filter = st.multiselect(
                        "상태", ["Todo", "In Progress", "Done"]
                    )
                with f3:
                    priority_filter = st.multiselect(
                        "우선순위", ["Low", "Medium", "High"]
                    )
                with f4:
                    tag_filter = st.text_input("태그(부분일치)")

                def apply_filters(df):
                    if df.empty:
                        return df
                    res = df.copy()
                    if assignee_filter:
                        res = res[
                            res["assignee"]
                            .fillna("")
                            .str.contains(assignee_filter, case=False)
                        ]
                    if status_filter:
                        res = res[res["status"].isin(status_filter)]
                    if priority_filter:
                        res = res[res["priority"].isin(priority_filter)]
                    if tag_filter:
                        res = res[
                            res["tags"]
                            .fillna("")
                            .str.contains(tag_filter, case=False)
                        ]
                    return res

            tdf_f = apply_filters(tdf) if not tdf.empty else tdf

            part_users_df = get_users_for_part(part_id)
            if not part_users_df.empty:
                user_options = ["(없음)"] + part_users_df["name"].tolist()
            else:
                user_options = ["(없음)"]

            col_todo, col_prog, col_done = st.columns(3)
            status_order = ["Todo", "In Progress", "Done"]

            for label, col in [
                ("Todo", col_todo),
                ("In Progress", col_prog),
                ("Done", col_done),
            ]:
                with col:
                    st.markdown(f"### {label}")
                    df_col = tdf_f[tdf_f["status"] == label]
                    if df_col.empty:
                        st.caption("비어 있음")
                    else:
                        for _, r in df_col.iterrows():
                            task_id = int(r["id"])
                            edit_key = f"edit_mode_{task_id}"
                            edit_mode = st.session_state.get(edit_key, False)

                            with st.container(border=True):
                                priority = r["priority"] or "Medium"
                                pr_label, pr_color = priority_label_and_color(
                                    priority
                                )

                                if not edit_mode:
                                    # ----- 보기 모드 -----
                                    st.markdown(
                                        f"""
                                        <div style="display:flex;align-items:center;gap:8px;">
                                          <span style="font-weight:600;">{r['title']}</span>
                                          <span style="font-size:0.8rem;padding:2px 8px;border-radius:999px;
                                                       background-color:{pr_color};color:#000;">
                                            {pr_label}
                                          </span>
                                        </div>
                                        """,
                                        unsafe_allow_html=True,
                                    )

                                    subtasks_orig = parse_subtasks(
                                        r.get("description") or ""
                                    )
                                    subtasks_for_view = []
                                    changed = False

                                    if subtasks_orig:
                                        if r["status"] == "Done":
                                            for (lbl, weight, done) in subtasks_orig:
                                                subtasks_for_view.append(
                                                    (lbl, weight, True)
                                                )
                                        else:
                                            subtasks_for_view = subtasks_orig[:]

                                        new_subtasks_after_click = []
                                        for i, (
                                            lbl,
                                            weight,
                                            done_flag,
                                        ) in enumerate(subtasks_for_view):
                                            key_cb = (
                                                f"view_sub_done_{task_id}_{i}_{r['status']}"
                                            )
                                            checked = st.checkbox(
                                                f"{lbl} ({weight}%)",
                                                value=done_flag,
                                                key=key_cb,
                                            )
                                            if checked != done_flag:
                                                changed = True
                                            new_subtasks_after_click.append(
                                                (lbl, weight, checked)
                                            )

                                        if changed:
                                            new_desc = serialize_subtasks(
                                                new_subtasks_after_click
                                            )
                                            new_prog = calc_progress_from_subtasks(
                                                new_subtasks_after_click
                                            )
                                            if new_prog == 0:
                                                new_status = "Todo"
                                            elif new_prog == 100:
                                                new_status = "Done"
                                            else:
                                                new_status = "In Progress"
                                            update_task(
                                                task_id,
                                                description=new_desc,
                                                progress=int(new_prog),
                                                status=new_status,
                                            )
                                            st.rerun()
                                    else:
                                        pass

                                    st.caption(
                                        f"담당: {r['assignee'] or '-'} · "
                                        f"마감: {r['due_date'] or '-'} · 진행률: {r['progress']}%"
                                    )

                                    b_done, b_edit, b_del = st.columns(
                                        3, gap="small"
                                    )
                                    with b_done:
                                        if st.button(
                                            "완료",
                                            key=f"done_btn_{task_id}",
                                            use_container_width=True,
                                        ):
                                            subtasks_all = parse_subtasks(
                                                r.get("description") or ""
                                            )
                                            if subtasks_all:
                                                new_subtasks_all = [
                                                    (lbl, w, True)
                                                    for (lbl, w, d) in subtasks_all
                                                ]
                                                new_desc = serialize_subtasks(
                                                    new_subtasks_all
                                                )
                                            else:
                                                new_desc = (
                                                    r.get("description") or None
                                                )
                                            update_task(
                                                task_id,
                                                status="Done",
                                                progress=100,
                                                description=new_desc,
                                            )
                                            st.rerun()
                                    with b_edit:
                                        if st.button(
                                            "수정",
                                            key=f"edit_btn_{task_id}",
                                            use_container_width=True,
                                        ):
                                            st.session_state[edit_key] = True
                                            st.rerun()
                                    with b_del:
                                        if st.button(
                                            "삭제",
                                            key=f"del_{task_id}",
                                            use_container_width=True,
                                        ):
                                            st.session_state[
                                                f"confirm_del_task_{task_id}"
                                            ] = True

                                    if st.session_state.get(
                                        f"confirm_del_task_{task_id}"
                                    ):
                                        st.warning(
                                            "정말 삭제할까요? 아래 버튼을 누르면 삭제됩니다."
                                        )
                                        c1, c2 = st.columns([1, 1])
                                        with c1:
                                            if st.button(
                                                "네, 삭제합니다",
                                                key=f"confirm_del_task_btn_{task_id}",
                                                use_container_width=True,
                                            ):
                                                delete_task(task_id)
                                                st.session_state.pop(
                                                    f"confirm_del_task_{task_id}",
                                                    None,
                                                )
                                                st.warning("작업이 삭제되었습니다.")
                                                st.rerun()
                                        with c2:
                                            if st.button(
                                                "취소",
                                                key=f"cancel_del_task_{task_id}",
                                                use_container_width=True,
                                            ):
                                                st.session_state.pop(
                                                    f"confirm_del_task_{task_id}",
                                                    None,
                                                )

                                else:
                                    # ----- 수정 모드 -----
                                    st.markdown("**수정 모드**")
                                    title_val = st.text_input(
                                        "제목",
                                        value=r["title"],
                                        key=f"edit_title_{task_id}",
                                    )

                                    assignee_current = r["assignee"] or "(없음)"
                                    assignee_val = st.selectbox(
                                        "담당자",
                                        user_options,
                                        index=user_options.index(assignee_current)
                                        if assignee_current in user_options
                                        else 0,
                                        key=f"edit_assignee_{task_id}",
                                    )

                                    subtasks = parse_subtasks(
                                        r.get("description") or ""
                                    )
                                    n_rows = max(len(subtasks), 1)
                                    edit_subtasks = []

                                    for i in range(n_rows):
                                        if i < len(subtasks):
                                            d_label, d_weight, d_done = subtasks[i]
                                        else:
                                            d_label, d_weight, d_done = "", 0, False
                                        c_l, c_p = st.columns([4, 1])
                                        with c_l:
                                            lbl = st.text_input(
                                                f"세부 작업 {i+1}",
                                                value=d_label,
                                                key=f"edit_sub_label_{task_id}_{i}",
                                            )
                                        with c_p:
                                            weight_val = st.number_input(
                                                "할당률 (%)",
                                                min_value=0,
                                                max_value=100,
                                                value=int(d_weight),
                                                key=f"edit_sub_prog_{task_id}_{i}",
                                            )
                                        if lbl.strip():
                                            edit_subtasks.append(
                                                (lbl.strip(), weight_val, d_done)
                                            )

                                    tags_val = st.text_input(
                                        "태그(쉼표 구분)",
                                        value=r.get("tags") or "",
                                        key=f"edit_tags_{task_id}",
                                    )

                                    b1, b2 = st.columns(2, gap="small")
                                    with b1:
                                        if st.button(
                                            "저장",
                                            key=f"save_edit_{task_id}",
                                            use_container_width=True,
                                        ):
                                            if edit_subtasks:
                                                new_desc = serialize_subtasks(
                                                    edit_subtasks
                                                )
                                                new_prog = (
                                                    calc_progress_from_subtasks(
                                                        edit_subtasks
                                                    )
                                                )
                                            else:
                                                new_desc = None
                                                new_prog = 0

                                            if new_prog == 0:
                                                new_status = "Todo"
                                            elif new_prog == 100:
                                                new_status = "Done"
                                            else:
                                                new_status = "In Progress"

                                            assignee_final = (
                                                None
                                                if assignee_val == "(없음)"
                                                else assignee_val
                                            )
                                            update_task(
                                                task_id,
                                                title=title_val.strip()
                                                or r["title"],
                                                status=new_status,
                                                description=new_desc,
                                                progress=int(new_prog),
                                                assignee=assignee_final,
                                                tags=tags_val.strip() or None,
                                            )
                                            st.session_state[edit_key] = False
                                            st.success("수정되었습니다.")
                                            st.rerun()
                                    with b2:
                                        if st.button(
                                            "취소",
                                            key=f"cancel_edit_{task_id}",
                                            use_container_width=True,
                                        ):
                                            st.session_state[edit_key] = False
                                            st.rerun()

            # -------- 새 작업 추가 --------
            st.divider()
            st.markdown("### ➕ 새 작업 추가")

            count_key = f"subtask_count_{part_id}"
            if count_key not in st.session_state:
                st.session_state[count_key] = 1

            with st.form(f"add_task_{part_id}"):
                c_title, c_tag = st.columns([2, 1])
                with c_title:
                    title = st.text_input(
                        "제목*",
                        placeholder="예: API 연동 구현",
                        key=f"title_input_{part_id}",
                    )
                with c_tag:
                    tags = st.text_input(
                        "태그(쉼표 구분)",
                        placeholder="백엔드,UI 등",
                        key=f"tag_input_{part_id}",
                    )

                c1, c2 = st.columns(2)
                with c1:
                    assignee_choice = st.selectbox(
                        "담당자", user_options, key=f"assignee_{part_id}"
                    )
                with c2:
                    status = st.selectbox(
                        "상태",
                        ["Todo", "In Progress", "Done"],
                        key=f"status_new_{part_id}",
                    )

                c3, c4 = st.columns(2)
                with c3:
                    start_date = st.date_input(
                        "시작일",
                        value=selected_day,
                        key=f"start_{part_id}",
                    )
                with c4:
                    due_date = st.date_input(
                        "마감일",
                        value=selected_day,
                        key=f"due_{part_id}",
                    )

                sub_labels = []
                sub_weights = []
                for i in range(st.session_state[count_key]):
                    c_l, c_p = st.columns([3, 1])
                    with c_l:
                        lbl = st.text_input(
                            f"세부 작업 {i+1}",
                            key=f"new_sub_label_{part_id}_{i}",
                        )
                    with c_p:
                        prog_val = st.number_input(
                            "할당률 (%)",
                            min_value=0,
                            max_value=100,
                            value=0,
                            key=f"new_sub_prog_{part_id}_{i}",
                        )
                    if lbl.strip():
                        sub_labels.append(lbl.strip())
                        sub_weights.append(prog_val)

                b1, b2 = st.columns(2, gap="small")
                add_clicked = b1.form_submit_button(
                    "세부 작업 추가", use_container_width=True
                )
                save_clicked = b2.form_submit_button(
                    "저장", use_container_width=True
                )

                if add_clicked:
                    st.session_state[count_key] += 1

                if save_clicked:
                    if not title.strip():
                        st.error("제목은 필수입니다.")
                    else:
                        if assignee_choice == "(없음)":
                            assignee_val = None
                        else:
                            assignee_val = assignee_choice

                        subtasks_new = []
                        for lbl, w in zip(sub_labels, sub_weights):
                            done_flag = True if status == "Done" else False
                            subtasks_new.append((lbl, w, done_flag))

                        if subtasks_new:
                            description_str = serialize_subtasks(subtasks_new)
                        else:
                            description_str = None

                        if status == "Done":
                            progress = 100
                        else:
                            progress = 0

                        insert_task(
                            project_id=selected_project_id,
                            part_id=part_id,
                            title=title.strip(),
                            description=description_str,
                            assignee=assignee_val,
                            priority="Medium",
                            status=status,
                            start_date=start_date.isoformat()
                            if start_date
                            else None,
                            due_date=due_date.isoformat()
                            if due_date
                            else None,
                            progress=int(progress),
                            tags=tags.strip() or None,
                        )
                        st.success("작업이 추가되었습니다.")
                        st.rerun()
