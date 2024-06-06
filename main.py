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
id_restaurantes = X_subway["id_restaurante"].tolist()
selected_id = st.selectbox("Seleccionar ID de Restaurante", id_restaurantes)

# Generar enlace con el formato especificado
enlace = f"https://ptf-data-subway.streamlit.app/?id={selected_id}"

st.write("Enlace:", enlace)

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
    tabs = [st.container() for _ in nombres_atributo.values()]
    for i, (atributo, nombre) in enumerate(nombres_atributo.items()):
        with tabs[i]:
            st.write(nombre)
            for tipo_atributo, value in data_2[atributo].items():
                check_key = f"{atributo}_{tipo_atributo}"
                is_checked = st.checkbox(
                    tipo_atributo, value=bool(value), key=check_key
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
