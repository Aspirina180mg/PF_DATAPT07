import streamlit as st
import pandas as pd
import pickle

st.set_page_config(layout="wide")
st.title("Restaurantes y Caraterísticas")

# Datos proporcionados
@st.cache_data
def read_data():
    X_subway = pd.read_parquet("ML/X_subway.parquet")
    X_subway_proc = pd.read_parquet("ML/X_subway_proc.parquet")
    filename = "ML/finalized_model.pickle"
    modelo = pickle.load(open(filename, "rb"))
    return X_subway, X_subway_proc, modelo

X_subway, X_subway_proc, modelo = read_data()

def get_atributos(id_restaurante):
    df = X_subway.query(f"id_restaurante == '{id_restaurante}'")
    tipo_atributo = [a.split("_")[0] for a in df.columns.tolist()[1:70]]
    atributo = [a.split("_")[1] for a in df.columns.tolist()[1:70]]
    atributos_dict = {
        "access": {},
        "amen": {},
        "atmos": {},
        "crowd": {},
        "dining": {},
        "health": {},
        "high": {},
        "offer": {},
        "pay": {},
        "popular": {},
        "service": {},
    }
    for ta, a in zip(tipo_atributo, atributo):
        atributos_dict[ta][a] = df.loc[:, f"{ta}_{a}"].values[0]
    return atributos_dict

# Dropdown con los id_restaurante
id_restaurantes = X_subway['id_restaurante'].tolist()

# Usamos 1/3 del espacio para el dropdown
with st.container():
    selected_id = st.selectbox("Seleccionar ID de Restaurante", id_restaurantes, key="dropdown", width=200)

# El resto del espacio es para el enlace
with st.container():
    enlace = f"<a href='https://ptf-data-subway.streamlit.app/?id={selected_id}' target='_self'>Enlace a restaurante</a>"
    st.write(enlace, unsafe_allow_html=True)


muestra = X_subway.query(f"id_restaurante == '{st.query_params['id']}'")

# st.dataframe(X_subway_proc.loc[muestra.index])
calificacion = {0: "😢 Mala", 1: "😀 Buena"}

st.write(
    calificacion[modelo.predict(X_subway_proc.loc[muestra.index])[0]],
    muestra["calificacion"].iloc[0],
)

data_2 = get_atributos(muestra["id_restaurante"].iloc[0])

nombres_atributo = {
    "access": "Accesibility",
    "amen": "Amenities",
    "atmos": "Atmosphere",
    "crowd": "Crowd",
    "dining": "Dining Options",
    "health": "Health and Safety",
    "high": "Highlights",
    "offer": "Offering",
    "pay": "Payment",
    "popular": "Popular for",
    "service": "Services",
}

with st.form("atributos_form"):
    submited_data = {}
    (
        t_access,
        t_amen,
        t_atmos,
        t_crowd,
        t_dining,
        t_health,
        t_high,
        t_offer,
        t_pay,
        t_popular,
        t_service,
    ) = st.tabs([i for i in nombres_atributo.values()])

    with t_access:
        c_access = st.container()

        with c_access:
            prefix = "access"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    "Wheelchair accessible entrance"
                    "Wheelchair accessible parking lot"
                    "Wheelchair accessible restroom"
                    "Wheelchair accessible seating"
                    "Wheelchair accessible elevator"
                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)

    with t_amen:
        c_amen = st.container()

        with c_amen:
            prefix = "amen"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo["amen"]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    "Bar on site"
                    "Gender-neutral restroom"
                    "Good for kids"
                    "High chairs"
                    "Restroom"
                    "Wi-Fi"

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)

    with t_atmos:
        c_atmos = st.container()
        with c_atmos:
            prefix = "atmos"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)

    with t_crowd:
        c_crowd = st.container()
        with c_crowd:
            prefix = "crowd"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)
    with t_dining:
        c_dining = st.container()
        with c_dining:
            prefix = "dining"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)
    with t_health:
        c_health = st.container()
        with c_health:
            prefix = "health"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)
    with t_high:
        c_high = st.container()
        with c_high:
            prefix = "high"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)
    with t_offer:
        c_offer = st.container()
        with c_offer:
            prefix = "offer"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)

    with t_pay:
        c_pay = st.container()
        with c_pay:
            prefix = "pay"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)
    with t_popular:
        c_popular = st.container()
        with c_popular:
            prefix = "popular"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)
    with t_service:
        c_service = st.container()
        with c_service:
            prefix = "service"
            col1, col2 = st.columns([1, 3])

            with col1:
                nombres_atributo[prefix]
            with col2:
                col_2a, col_2b = st.columns(2)
                with col_2a:
                    for tipo_atributo in data_2[prefix].items():
                        st.write(f"{tipo_atributo[0]}")

                with col_2b:
                    for tipo_atributo in data_2[prefix].items():
                        check_key = f"{prefix}_{tipo_atributo[0]}"
                        is_checked = col_2b.checkbox(
                            "",
                            key=check_key,
                            value=bool(data_2[prefix][tipo_atributo[0]]),
                        )
                        submited_data[check_key] = int(is_checked)

    submitted = st.form_submit_button("Evaluar")

    if submitted:
        df = pd.DataFrame([submited_data])
        X_subway_proc.iloc[muestra.index, 0:69] = df.iloc[0, 0:69]
        actualizado = X_subway_proc.iloc[muestra.index].copy()
        st.write(calificacion[int(muestra["calificacion"].iloc[0] >= 4.02)])
        st.write("Luego de las modificaciones: ")
        st.write(calificacion[modelo.predict(actualizado)[0]])
        if modelo.predict(actualizado)[0] > 0:
            st.balloons()
