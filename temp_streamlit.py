import streamlit as st

MPG_TO_KML = 0.425144  # 1 mpg = 0.425144 km/L

st.set_page_config(page_title="MPG ↔ KM/L Converter", layout="centered")
st.title("MPG ↔ KM/L 변환기")

col_in, col_btns, col_out = st.columns([1, 2, 1])

with col_in:
    val_str = st.text_input("입력값", value="", placeholder="숫자만 입력")

with col_btns:
    st.write(" ")
    st.write(" ")
    c1, c2 = st.columns(2)
    mpg2kml = c1.button("mpg → km/L")
    kml2mpg = c2.button("km/L → mpg")

with col_out:
    st.write("출력값")
    out_box = st.empty()

def is_number(s: str):
    try:
        float(s)
        return True
    except:
        return False

if mpg2kml or kml2mpg:
    if not is_number(val_str):
        out_box.error("유효한 숫자를 입력하세요.")
    else:
        v = float(val_str)
        if mpg2kml:
            kml = v * MPG_TO_KML
            out_box.success(f"{kml:.4f}")
        elif kml2mpg:
            mpg = v / MPG_TO_KML
            out_box.success(f"{mpg:.4f}")
