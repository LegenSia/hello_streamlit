import streamlit as st
import random

st.set_page_config(page_title="Streamlit Sudoku", layout="centered")
st.title("Streamlit Sudoku")

def base_solution():
    side, base = 9, 3
    return [[(i*base + i//base + j) % side + 1 for j in range(side)] for i in range(side)]

def shuffled_solution():
    rows = list(range(9))
    cols = list(range(9))
    nums = list(range(1, 10))

    def shuffle_in_groups(idxs):
        groups = [idxs[x:x+3] for x in range(0, 9, 3)]
        random.shuffle(groups)
        for g in groups:
            random.shuffle(g)
        return [i for g in groups for i in g]

    rows = shuffle_in_groups(rows)
    cols = shuffle_in_groups(cols)
    random.shuffle(nums)
    base = base_solution()
    return [[nums[base[r][c]-1] for c in cols] for r in rows]

def make_puzzle(solution, diff):
    keep_map = {"쉬움": 45, "중간": 35, "어려움": 28}
    keep = keep_map.get(diff, 35)
    cells = [(r, c) for r in range(9) for c in range(9)]
    random.shuffle(cells)
    p = [[solution[r][c] for c in range(9)] for r in range(9)]
    blanks = 81 - keep
    for (r, c) in cells[:blanks]:
        p[r][c] = 0
    return p

def check_complete(grid):
    target = set(range(1, 10))
    for r in range(9):
        row = grid[r]
        if 0 in row or set(row) != target:
            return False
    for c in range(9):
        col = [grid[r][c] for r in range(9)]
        if set(col) != target:
            return False
    for br in range(0, 9, 3):
        for bc in range(0, 9, 3):
            box = [grid[r][c] for r in range(br, br+3) for c in range(bc, bc+3)]
            if set(box) != target:
                return False
    return True

# session state
if "solution" not in st.session_state:
    st.session_state.solution = shuffled_solution()
if "puzzle" not in st.session_state:
    st.session_state.puzzle = make_puzzle(st.session_state.solution, "중간")
if "user_grid" not in st.session_state:
    st.session_state.user_grid = [[st.session_state.puzzle[r][c] for c in range(9)] for r in range(9)]
if "difficulty" not in st.session_state:
    st.session_state.difficulty = "중간"
if "hint_used" not in st.session_state:
    st.session_state.hint_used = 0

HINT_LIMIT = 3

def new_game(diff):
    st.session_state.difficulty = diff
    st.session_state.solution = shuffled_solution()
    st.session_state.puzzle = make_puzzle(st.session_state.solution, diff)
    st.session_state.user_grid = [[st.session_state.puzzle[r][c] for c in range(9)] for r in range(9)]
    st.session_state.hint_used = 0

def reset_board():
    st.session_state.user_grid = [[st.session_state.puzzle[r][c] for c in range(9)] for r in range(9)]

def give_hint():
    if st.session_state.hint_used >= HINT_LIMIT:
        return
    sol = st.session_state.solution
    puzz = st.session_state.puzzle
    user = st.session_state.user_grid
    blanks = [(r, c) for r in range(9) for c in range(9) if puzz[r][c] == 0 and user[r][c] == 0]
    if blanks:
        r, c = random.choice(blanks)
        user[r][c] = sol[r][c]
        st.session_state.hint_used += 1
        return
    wrongs = [(r, c) for r in range(9) for c in range(9)
              if puzz[r][c] == 0 and user[r][c] != 0 and user[r][c] != sol[r][c]]
    if wrongs:
        r, c = random.choice(wrongs)
        user[r][c] = sol[r][c]
        st.session_state.hint_used += 1

# controls
d1, d2, d3, sp, r1, h1 = st.columns([1, 1, 1, 3, 1, 1])

with d1:
    if st.button("쉬움"):
        new_game("쉬움")
with d2:
    if st.button("중간"):
        new_game("중간")
with d3:
    if st.button("어려움"):
        new_game("어려움")

with r1:
    if st.button("리셋"):
        reset_board()

with h1:
    if st.button("힌트"):
        give_hint()
    remain = HINT_LIMIT - st.session_state.hint_used
    st.caption(f"힌트: {remain}/{HINT_LIMIT}")

st.caption(f"난이도: {st.session_state.difficulty}")

# board
puzzle = st.session_state.puzzle
user = st.session_state.user_grid

for r in range(9):
    cols = st.columns(9, gap="small")
    for c in range(9):
        given = puzzle[r][c] != 0
        val = user[r][c]
        key = f"cell_{r}_{c}"
        txt = cols[c].text_input(
            label="",
            value=str(puzzle[r][c]) if given else (str(val) if val != 0 else ""),
            key=key,
            disabled=given,
            label_visibility="collapsed",
            placeholder=""
        )
        if not given:
            s = (txt or "").strip()
            s = s[:1]
            user[r][c] = int(s) if s.isdigit() and s != "0" else 0
    if r in (2, 5):
        st.write("")

# 정답 확인 버튼 + 결과 메시지
st.markdown("---")
if st.button("정답 확인"):
    if check_complete(st.session_state.user_grid):
        st.success("Clear! 정답을 모두 맞췄습니다!")
    else:
        st.warning("아직 정답이 아닙니다.")
