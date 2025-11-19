import streamlit as st
import pandas as pd
import psycopg2
from contextlib import closing
from datetime import datetime, date
from streamlit_calendar import calendar
from st_circular_progress import CircularProgress

# =========================================================
# DB 연결 (Supabase Transaction Pooler + keepalive)
# =========================================================
def get_conn():
    if "postgres" not in st.secrets:
        st.error("Postgres 설정이 없습니다. Secrets에 [postgres] 섹션을 추가하세요.")
        st.stop()

    cfg = st.secrets["postgres"]
    return psycopg2.connect(
        host=cfg["host"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        port=cfg.get("port", 5432),
        sslmode="require",
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def safe_read_df(sql: str, params=None, retries: int = 2):
    """Supabase 연결 끊김(SSL closed 등) 재시도용 래퍼"""
    last_err = None
    for _ in range(retries):
        try:
            with closing(get_conn()) as conn:
                return pd.read_sql_query(sql, conn, params=params)
        except psycopg2.Error as e:
            msg = str(e)
            if "SSL connection has been closed" in msg or "could not connect" in msg:
                last_err = e
                continue
            raise
    raise last_err


# =========================================================
# init / seed 는 이제 실제 DB 스키마가 이미 맞춰져 있으므로 최소화
# =========================================================
def init_db():
    # Supabase에 이미 테이블/스키마를 만들어뒀으므로 여기서는 NO-OP
    return


def seed_if_empty():
    # Supabase SQL로 기본 프로젝트/파트/유저를 넣어둔 상태면 NO-OP
    # 필요하면 최소 샘플만 넣도록 구현할 수 있음
    return


# =========================================================
# 공통 유틸
# =========================================================
def parse_subtasks(description: str):
    """
    description 형태: "라벨|가중치|done\n라벨2|60|0" …
    """
    result = []
    if not description:
        return result
    for line in description.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 3:
            continue
        label = parts[0].strip()
        try:
            weight = int(parts[1])
        except ValueError:
            weight = 0
        done_flag = str(parts[2]).strip()
        done = done_flag in ("1", "True", "true", "YES", "yes")
        weight = max(0, min(100, weight))
        result.append((label, weight, done))
    return result


def build_description(subtasks):
    """
    subtasks: [(label, weight, done_bool), ...] -> description 문자열
    """
    lines = []
    for label, weight, done in subtasks:
        done_flag = "1" if done else "0"
        lines.append(f"{label}|{weight}|{done_flag}")
    return "\n".join(lines)


def calc_progress_from_subtasks(subtasks):
    if not subtasks:
        return 0
    total = 0
    for _, weight, done in subtasks:
        if done:
            total += weight
    total = max(0, min(100, total))
    return total


def status_from_progress(pct: int):
    if pct <= 0:
        return "Todo"
    elif pct >= 100:
        return "Done"
    else:
        return "Progress"


# =========================================================
# DB 액세스 함수들
# =========================================================
def list_projects():
    return safe_read_df("SELECT * FROM projects ORDER BY id")


def list_parts(project_id=None):
    if project_id:
        return safe_read_df(
            "SELECT * FROM parts WHERE project_id = %s ORDER BY id", [project_id]
        )
    return safe_read_df("SELECT * FROM parts ORDER BY id")


def list_users():
    query = """
        SELECT u.*,
               COALESCE(string_agg(p.name, ', ' ORDER BY p.id), '') AS part_names
        FROM users u
        LEFT JOIN user_parts up ON up.user_id = u.id
        LEFT JOIN parts p ON p.id = up.part_id
        GROUP BY u.id
        ORDER BY u.id
    """
    return safe_read_df(query)


def list_user_parts(user_id):
    return safe_read_df(
        """
        SELECT p.*
        FROM user_parts up
        JOIN parts p ON p.id = up.part_id
        WHERE up.user_id = %s
        ORDER BY p.id
        """,
        [user_id],
    )


def list_user_projects(user_id):
    # 접속 가능한 프로젝트를 user_projects 테이블 대신
    # parts → projects를 통해 유추
    return safe_read_df(
        """
        SELECT DISTINCT pr.*
        FROM user_parts up
        JOIN parts pa ON pa.id = up.part_id
        JOIN projects pr ON pr.id = pa.project_id
        WHERE up.user_id = %s
        ORDER BY pr.id
        """,
        [user_id],
    )


def list_tasks(project_id=None, part_id=None, assignee_id=None, tag_filter=None):
    base = """
        SELECT t.*, p.name AS part_name, p.color AS part_color
        FROM tasks t
        JOIN parts p ON p.id = t.part_id
    """
    params = []
    conds = []
    if project_id:
        conds.append("t.project_id = %s")
        params.append(project_id)
    if part_id:
        conds.append("t.part_id = %s")
        params.append(part_id)
    if assignee_id:
        conds.append("t.assignee_id = %s")
        params.append(assignee_id)
    if tag_filter:
        conds.append("t.tags ILIKE %s")
        params.append(f"%{tag_filter}%")
    if conds:
        base += " WHERE " + " AND ".join(conds)
    base += " ORDER BY t.due_date IS NULL, t.due_date ASC, t.id DESC"
    return safe_read_df(base, params)


def list_subtasks(task_id):
    return safe_read_df(
        "SELECT * FROM subtasks WHERE task_id = %s ORDER BY id", [task_id]
    )


def list_schedules(project_id=None, part_id=None):
    base = "SELECT * FROM schedules"
    params = []
    conds = []
    if project_id:
        conds.append("project_id = %s")
        params.append(project_id)
    if part_id:
        conds.append("part_id = %s")
        params.append(part_id)
    if conds:
        base += " WHERE " + " AND ".join(conds)
    base += " ORDER BY start_date"
    return safe_read_df(base, params)


def insert_task(
    project_id,
    part_id,
    title,
    description,
    assignee_name,
    priority,
    status,
    start_date,
    due_date,
    progress,
    tags,
    assignee_id,
):
    now = datetime.utcnow().isoformat()
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tasks(
                project_id, part_id, title, description, assignee,
                priority, status, start_date, due_date, progress,
                tags, assignee_id, created_at, updated_at
            ) VALUES (
                %(project_id)s, %(part_id)s, %(title)s, %(description)s, %(assignee)s,
                %(priority)s, %(status)s, %(start_date)s, %(due_date)s, %(progress)s,
                %(tags)s, %(assignee_id)s, %(created_at)s, %(updated_at)s
            )
            """,
            dict(
                project_id=project_id,
                part_id=part_id,
                title=title,
                description=description,
                assignee=assignee_name,
                priority=priority,
                status=status,
                start_date=start_date,
                due_date=due_date,
                progress=progress,
                tags=tags,
                assignee_id=assignee_id,
                created_at=now,
                updated_at=now,
            ),
        )
        conn.commit()


def update_task_from_subtasks(task_row, subtasks, priority=None, tags=None):
    progress = calc_progress_from_subtasks(subtasks)
    status = status_from_progress(progress)
    desc = build_description(subtasks)
    now = datetime.utcnow().isoformat()
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE tasks
            SET description = %(description)s,
                priority = COALESCE(%(priority)s, priority),
                tags = COALESCE(%(tags)s, tags),
                progress = %(progress)s,
                status = %(status)s,
                updated_at = %(updated_at)s
            WHERE id = %(id)s
            """,
            dict(
                id=int(task_row["id"]),
                description=desc,
                priority=priority,
                tags=tags,
                progress=progress,
                status=status,
                updated_at=now,
            ),
        )
        conn.commit()


def delete_task(task_id):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM subtasks WHERE task_id = %s", [task_id])
        cur.execute("DELETE FROM tasks WHERE id = %s", [task_id])
        conn.commit()


def insert_schedule(project_id, part_id, title, start_date, end_date, color):
    now = datetime.utcnow().isoformat()
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO schedules(project_id, part_id, title, start_date, end_date, color, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            [project_id, part_id, title, start_date, end_date, color, now],
        )
        conn.commit()


# 유저/파트/프로젝트 관리용 간단한 함수들
def insert_project(name):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO projects(name) VALUES (%s)", [name])
        conn.commit()


def rename_project(project_id, name):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE projects SET name = %s WHERE id = %s", [name, project_id])
        conn.commit()


def insert_part(project_id, name, color="#cccccc"):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO parts(project_id, name, color) VALUES (%s,%s,%s)",
            [project_id, name, color],
        )
        conn.commit()


def update_part(part_id, name=None, color=None):
    sets = []
    params = []
    if name is not None:
        sets.append("name = %s")
        params.append(name)
    if color is not None:
        sets.append("color = %s")
        params.append(color)
    if not sets:
        return
    params.append(part_id)
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE parts SET {', '.join(sets)} WHERE id = %s", params)
        conn.commit()


def insert_user(name, email, part_ids, role):
    now = datetime.utcnow().isoformat()
    main_part_id = part_ids[0] if part_ids else None
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users(name,email,part_id,role,created_at) VALUES (%s,%s,%s,%s,%s) RETURNING id",
            [name, email, main_part_id, role, now],
        )
        user_id = cur.fetchone()[0]
        for pid in part_ids:
            cur.execute(
                "INSERT INTO user_parts(user_id,part_id) VALUES (%s,%s)", [user_id, pid]
            )
        conn.commit()


def update_user_parts(user_id, part_ids):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_parts WHERE user_id = %s", [user_id])
        for pid in part_ids:
            cur.execute(
                "INSERT INTO user_parts(user_id,part_id) VALUES (%s,%s)", [user_id, pid]
            )
        conn.commit()


def delete_user(user_id):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM user_parts WHERE user_id = %s", [user_id])
        cur.execute("DELETE FROM users WHERE id = %s", [user_id])
        conn.commit()


# =========================================================
# 로그인 / 권한
# =========================================================
def show_login():
    st.title("로그인")

    company = st.text_input("회사명")
    username = st.text_input("아이디", key="login_username")
    password = st.text_input("비밀번호", type="password")

    auth_cfg = st.secrets.get("auth", {})
    company_name = auth_cfg.get("company_name", "Inha")
    admin_username = auth_cfg.get("admin_username", "admin")
    admin_password = auth_cfg.get("admin_password", "1234")
    user_username = auth_cfg.get("user_username", "user")
    user_password = auth_cfg.get("user_password", "1234")

    col_login, _ = st.columns([1, 3])
    login_btn = col_login.button("로그인", use_container_width=True)

    if login_btn:
        if company != company_name:
            st.error("회사명이 올바르지 않습니다.")
            return
        role = None
        if username == admin_username and password == admin_password:
            role = "admin"
        elif username == user_username and password == user_password:
            role = "user"

        if not role:
            st.error("아이디 또는 비밀번호가 올바르지 않습니다.")
            return

        st.session_state["logged_in"] = True
        st.session_state["user_role"] = role
        st.session_state["login_username"] = username
        st.experimental_rerun()


# =========================================================
# UI 구성
# =========================================================
def render_dashboard(current_user_id, current_project_id):
    st.subheader("대시보드")

    # 프로젝트의 모든 파트 일정 조회
    parts_df = list_parts(current_project_id)
    schedules_df = list_schedules(current_project_id, None)

    events = []
    for _, row in schedules_df.iterrows():
        events.append(
            {
                "title": row["title"],
                "start": row["start_date"],
                "end": row["end_date"],
                "color": row.get("color") or "#87CEFA",
            }
        )

    cal = calendar(
        events=events,
        options={"initialView": "dayGridMonth"},
        custom_css="""
        .fc {
            font-size: 12px;
        }
        """,
        key="dashboard_calendar",
    )

    # 나의 할 일 브리핑
    st.markdown("---")
    st.markdown("### 나의 할 일 브리핑")

    my_tasks_df = list_tasks(project_id=current_project_id, assignee_id=current_user_id)
    if my_tasks_df.empty:
        st.write("현재 할 일이 없습니다.")
    else:
        for _, t in my_tasks_df.iterrows():
            st.write(
                f"- [{t['status']}] {t['title']} (마감: {t.get('due_date') or '-'}, 진행률: {t['progress']}%)"
            )

    # 진행률 원형 그래프
    st.markdown("### 전체 / 파트 진행률")
    col_all, col_parts = st.columns([1, 2])

    # 전체
    with col_all:
        all_tasks = list_tasks(project_id=current_project_id)
        if all_tasks.empty:
            pct_done = 0
        else:
            # 전체 진행률 = Done 비율
            total_cnt = len(all_tasks)
            done_cnt = (all_tasks["status"] == "Done").sum()
            pct_done = int(done_cnt / total_cnt * 100)
        circ = CircularProgress(
            value=pct_done,
            size=120,
            thickness=10,
            label="전체",
        )
        circ.render()

    # 파트별
    with col_parts:
        cols = st.columns(max(1, len(parts_df)))
        for (idx, row), c in zip(parts_df.iterrows(), cols):
            part_tasks = my_tasks_df[my_tasks_df["part_id"] == row["id"]]
            if part_tasks.empty:
                pct = 0
            else:
                total_cnt = len(part_tasks)
                done_cnt = (part_tasks["status"] == "Done").sum()
                pct = int(done_cnt / total_cnt * 100)
            with c:
                circ = CircularProgress(
                    value=pct, size=100, thickness=8, label=row["name"]
                )
                circ.render()


def render_part_board(project_id, part_row, current_user_id):
    st.markdown(f"### {part_row['name']} 파트 보드")

    tag_filter = st.text_input("태그 검색", key=f"tag_filter_{part_row['id']}")
    df = list_tasks(
        project_id=project_id,
        part_id=int(part_row["id"]),
        assignee_id=None,
        tag_filter=tag_filter or None,
    )

    todo_df = df[df["status"] == "Todo"]
    prog_df = df[df["status"] == "Progress"]
    done_df = df[df["status"] == "Done"]

    col_todo, col_prog, col_done = st.columns(3)

    def render_task_card(t_row, container):
        subtasks = parse_subtasks(t_row.get("description") or "")
        if not subtasks:
            # 서브없으면 100% 한 개
            subtasks = [(t_row["title"], 100, t_row["status"] == "Done")]

        with container:
            st.markdown(
                f"**{t_row['title']}**  \n담당: {t_row.get('assignee') or '-'} · 마감: {t_row.get('due_date') or '-'} · 진행률: {t_row['progress']}%"
            )

            # 서브작업 체크박스
            new_subtasks = []
            for idx, (label, weight, done) in enumerate(subtasks):
                key_cb = f"sub_{t_row['id']}_{idx}"
                checked = st.checkbox(
                    f"{label} ({weight}%)", value=done, key=key_cb
                )
                new_subtasks.append((label, weight, checked))

            # 버튼들
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("완료", key=f"complete_{t_row['id']}", use_container_width=True):
                    # 모든 서브 체크
                    all_sub = [(lbl, w, True) for (lbl, w, _) in new_subtasks]
                    update_task_from_subtasks(t_row, all_sub)
                    st.experimental_rerun()
            with c2:
                if st.button("수정", key=f"edit_{t_row['id']}", use_container_width=True):
                    st.session_state["edit_task_id"] = int(t_row["id"])
                    st.experimental_rerun()
            with c3:
                if st.button("삭제", key=f"del_{t_row['id']}", use_container_width=True):
                    delete_task(int(t_row["id"]))
                    st.experimental_rerun()

            # 체크박스 변경 반영
            progress_now = calc_progress_from_subtasks(new_subtasks)
            status_now = status_from_progress(progress_now)
            if (
                progress_now != int(t_row["progress"])
                or status_now != t_row["status"]
            ):
                update_task_from_subtasks(t_row, new_subtasks)
                st.experimental_rerun()

    with col_todo:
        st.markdown("#### Todo")
        if todo_df.empty:
            st.caption("없음")
        else:
            for _, row in todo_df.iterrows():
                render_task_card(row, st.container())
    with col_prog:
        st.markdown("#### In Progress")
        if prog_df.empty:
            st.caption("없음")
        else:
            for _, row in prog_df.iterrows():
                render_task_card(row, st.container())
    with col_done:
        st.markdown("#### Done")
        if done_df.empty:
            st.caption("없음")
        else:
            for _, row in done_df.iterrows():
                render_task_card(row, st.container())

    st.markdown("---")
    st.markdown("#### 새 작업 추가")

    c1, c2 = st.columns([2, 1])
    with c1:
        title = st.text_input("제목", key=f"new_title_{part_row['id']}")
    with c2:
        tags = st.text_input("태그", key=f"new_tags_{part_row['id']}")

    assignee_name = st.text_input("담당자", key=f"new_assignee_{part_row['id']}")
    priority = st.selectbox(
        "우선순위",
        ["낮음", "중간", "높음"],
        index=1,
        key=f"new_prio_{part_row['id']}",
    )
    c3, c4 = st.columns(2)
    with c3:
        start_date = st.date_input(
            "시작일", value=date.today(), key=f"new_start_{part_row['id']}"
        )
    with c4:
        due_date = st.date_input(
            "마감일", value=date.today(), key=f"new_due_{part_row['id']}"
        )

    st.markdown("세부 작업 (라벨 | 할당률)")
    sub_label = st.text_input(
        "세부 작업 1 라벨", key=f"sub1_label_{part_row['id']}"
    )
    sub_weight = st.number_input(
        "세부 작업 1 할당률(%)", 0, 100, 100, key=f"sub1_weight_{part_row['id']}"
    )

    if st.button("저장", key=f"save_new_task_{part_row['id']}", use_container_width=True):
        if not title:
            st.warning("제목을 입력하세요.")
        else:
            subtasks = []
            if sub_label:
                subtasks.append((sub_label, int(sub_weight), False))
            description = build_description(subtasks)
            progress = calc_progress_from_subtasks(subtasks)
            status = status_from_progress(progress)
            insert_task(
                project_id=project_id,
                part_id=int(part_row["id"]),
                title=title,
                description=description,
                assignee_name=assignee_name,
                priority=priority,
                status=status,
                start_date=start_date.isoformat(),
                due_date=due_date.isoformat(),
                progress=progress,
                tags=tags,
                assignee_id=current_user_id,
            )
            st.success("작업이 추가되었습니다.")
            st.experimental_rerun()


def render_admin(project_id):
    st.markdown("### 관리자")

    tab_proj, tab_user = st.tabs(["프로젝트 관리", "유저 관리"])

    with tab_proj:
        st.markdown("#### 프로젝트 관리")
        projects_df = list_projects()
        col_l, col_r = st.columns(2)

        with col_l:
            st.write("프로젝트 목록")
            for _, row in projects_df.iterrows():
                st.write(f"- {row['id']}: {row['name']}")

            new_proj_name = st.text_input("새 프로젝트 이름", key="admin_new_proj")
            if st.button("프로젝트 추가", key="btn_add_proj"):
                if new_proj_name:
                    insert_project(new_proj_name)
                    st.experimental_rerun()

        with col_r:
            st.write("현재 프로젝트 파트 관리")
            if not project_id:
                st.info("좌측에서 프로젝트를 먼저 선택하세요.")
            else:
                parts_df = list_parts(project_id)
                for _, row in parts_df.iterrows():
                    c1, c2 = st.columns([2, 1])
                    with c1:
                        new_name = st.text_input(
                            f"이름_{row['id']}", value=row["name"], key=f"edit_pname_{row['id']}"
                        )
                    with c2:
                        new_color = st.color_picker(
                            f"색상_{row['id']}", value=row["color"], key=f"edit_pcolor_{row['id']}"
                        )
                    if (
                        new_name != row["name"]
                        or new_color != row["color"]
                    ):
                        if st.button(
                            "저장",
                            key=f"save_part_{row['id']}",
                            use_container_width=True,
                        ):
                            update_part(int(row["id"]), new_name, new_color)
                            st.experimental_rerun()

                st.markdown("---")
                st.write("새 파트 추가")
                np_name = st.text_input("파트 이름", key="admin_new_part_name")
                np_color = st.color_picker(
                    "파트 색상", value="#cccccc", key="admin_new_part_color"
                )
                if st.button("파트 추가", key="admin_add_part"):
                    if np_name:
                        insert_part(project_id, np_name, np_color)
                        st.experimental_rerun()

    with tab_user:
        st.markdown("#### 유저 관리")
        users_df = list_users()
        parts_df = list_parts()

        st.write("유저 목록")
        for _, u in users_df.iterrows():
            st.write(f"- {u['id']}: {u['name']} ({u.get('part_names') or ''})")

        st.markdown("---")
        st.write("유저 추가")
        nu_name = st.text_input("이름", key="new_user_name")
        nu_email = st.text_input("이메일", key="new_user_email")
        nu_role = st.text_input("역할", key="new_user_role")
        nu_parts = st.multiselect(
            "소속 파트",
            options=[int(r["id"]) for _, r in parts_df.iterrows()],
            format_func=lambda pid: parts_df.set_index("id").loc[pid]["name"],
            key="new_user_parts",
        )
        if st.button("유저 추가", key="btn_new_user"):
            if nu_name:
                insert_user(nu_name, nu_email, nu_parts, nu_role)
                st.experimental_rerun()

        st.markdown("---")
        st.write("유저 상세 설정")
        if not users_df.empty:
            user_ids = [int(r["id"]) for _, r in users_df.iterrows()]
            sel_uid = st.selectbox(
                "유저 선택",
                user_ids,
                format_func=lambda uid: users_df.set_index("id").loc[uid]["name"],
                key="edit_user_select",
            )
            if sel_uid:
                sel_user = users_df.set_index("id").loc[sel_uid]
                st.write(f"선택된 유저: {sel_user['name']}")

                curr_parts = list_user_parts(sel_uid)
                curr_part_ids = [int(r["id"]) for _, r in curr_parts.iterrows()]
                new_parts = st.multiselect(
                    "소속 파트 수정",
                    options=[int(r["id"]) for _, r in parts_df.iterrows()],
                    default=curr_part_ids,
                    format_func=lambda pid: parts_df.set_index("id").loc[pid]["name"],
                    key="edit_user_parts",
                )
                c1, c2 = st.columns(2)
                with c1:
                    if st.button(
                        "저장 및 수정",
                        key="btn_user_save",
                        use_container_width=True,
                    ):
                        update_user_parts(sel_uid, new_parts)
                        st.experimental_rerun()
                with c2:
                    if st.button(
                        "유저 삭제",
                        key="btn_user_delete",
                        use_container_width=True,
                    ):
                        delete_user(sel_uid)
                        st.experimental_rerun()


# =========================================================
# 메인 앱
# =========================================================
def main():
    st.set_page_config(
        page_title="일정·진행도 관리 협업툴",
        layout="wide",
    )

    if "db_initialized" not in st.session_state:
        init_db()
        seed_if_empty()
        st.session_state["db_initialized"] = True

    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if not st.session_state["logged_in"]:
        show_login()
        return

    # 로그인된 상태
    user_role = st.session_state.get("user_role", "user")

    # 프로젝트 / 화면 선택
    projects_df = list_projects()
    sidebar = st.sidebar
    sidebar.markdown("#### 프로젝트")
    if projects_df.empty:
        sidebar.write("프로젝트가 없습니다.")
        current_project_id = None
    else:
        proj_options = [int(r["id"]) for _, r in projects_df.iterrows()]
        def proj_name(pid): return projects_df.set_index("id").loc[pid]["name"]
        current_project_id = sidebar.selectbox(
            "",
            proj_options,
            format_func=proj_name,
            key="sidebar_project",
        )

    sidebar.markdown("---")
    sidebar.markdown("#### 파트")
    part_view = sidebar.radio(
        "",
        options=["Dashboard", "기획", "개발", "아트"],
        key="sidebar_view",
    )

    if user_role == "admin":
        sidebar.markdown("---")
        sidebar.markdown("#### 관리자")
        admin_mode = sidebar.checkbox("관리자 메뉴 보기", value=False)
    else:
        admin_mode = False

    sidebar.markdown("---")
    if sidebar.button("로그아웃", use_container_width=True):
        st.session_state.clear()
        st.experimental_rerun()

    # 메인 컨텐츠
    if current_project_id is None:
        st.write("프로젝트를 먼저 생성하거나 선택하세요.")
        return

    # 임시로 admin 계정 → 유저 ID 1, user 계정 → 유저 ID 1 로 둔다
    current_user_id = 1

    project_name = projects_df.set_index("id").loc[current_project_id]["name"]
    st.markdown(f"## {project_name}")

    if part_view == "Dashboard":
        render_dashboard(current_user_id, current_project_id)
    else:
        # 파트명과 매칭
        parts_df = list_parts(current_project_id)
        part_map = {row["name"]: row for _, row in parts_df.iterrows()}
        part_row = part_map.get(part_view)
        if part_row is None:
            st.write("해당 파트가 존재하지 않습니다.")
        else:
            render_part_board(current_project_id, part_row, current_user_id)

    if admin_mode:
        st.markdown("---")
        render_admin(current_project_id)


if __name__ == "__main__":
    main()
