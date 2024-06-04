import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os
import re
import joblib

######## Machine Learning

from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier, GradientBoostingRegressor, VotingClassifier, GradientBoostingClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay, mean_squared_error, accuracy_score, classification_report
from sklearn.model_selection import train_test_split, cross_validate, GridSearchCV, cross_val_score, cross_val_predict
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import make_pipeline, Pipeline
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, StandardScaler, MaxAbsScaler, MinMaxScaler
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

from xgboost import XGBClassifier


# print("Leemos un modelo de machine learning y lo implementamos")

# loaded_pipeline = joblib.load('model_pipeline.pkl')

BASE_PATH = 'Datawarehouse'
SITIOS_FILE_URL = 'restaurantes.parquet'




def preprocess(data_frame):

    data_frame.loc[:, 'origen'] =  data_frame.loc[:, 'id_restaurante'].apply(lambda x: 1 if len(x) > 22 else 0)
    data_frame['id_estado'] = data_frame['id_estado'].apply(lambda x: int(x))

    columnas = ['Accessibility',
    'Amenities',
    'Atmosphere',
    'Crowd',
    'Dining options',
    'Health & safety',
    'Highlights',
    'Offerings',
    'Payments',
    'Popular for',
    'Service options']

    atributos = columnas
    df_atributos = pd.json_normalize(data_frame.atributos).reset_index()[atributos].fillna(np.ndarray([]))

    for col in atributos:
        df_atributos[col] = df_atributos[col].apply(str)
        df_atributos[col] = df_atributos[col].apply(lambda c: "[]" if c == '0.0' else c)


    df_atributos['Accessibility'] = df_atributos['Accessibility'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_accesibilidad =  df_atributos['Accessibility'].explode().unique()[1:].tolist()
    df_accesibilidad = pd.DataFrame([[False] * len(columnas_accesibilidad)] * df_atributos.shape[0], columns=columnas_accesibilidad)

    for i, row in enumerate(df_atributos['Accessibility']):
        for j, val in enumerate(row):
            df_accesibilidad.loc[i, val] = True

    data_frame = data_frame.reset_index().reset_index().drop(columns=['index']).rename({'level_0': 'index'}, axis=1, errors='ignore')
    df_accesibilidad.reset_index().merge(data_frame, on="index").sample(3)

    df_accesibilidad.columns = ["access_" + c for c in df_accesibilidad.columns]
    union = df_accesibilidad.reset_index().merge(data_frame, on="index")

    df_atributos['Amenities'] = df_atributos['Amenities'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_amenities = df_atributos['Amenities'].explode().unique()[1:].tolist()
    df_amenities = pd.DataFrame([[False] * len(columnas_amenities)] * df_atributos.shape[0], columns=columnas_amenities)

    for i, row in enumerate(df_atributos['Amenities']):
        for j, val in enumerate(row):
            df_amenities.loc[i, val] = True

    df_amenities.columns = ["amen_" + c for c in df_amenities.columns]
    union = df_amenities.reset_index().merge(union, on="index")

    df_atributos['Atmosphere'] = df_atributos['Atmosphere'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_atmosphere = df_atributos['Atmosphere'].explode().unique()[1:].tolist()

    df_atmosphere = pd.DataFrame([[False] * len(columnas_atmosphere)] * df_atributos.shape[0], columns=columnas_atmosphere)

    for i, row in enumerate(df_atributos['Atmosphere']):
        for j, val in enumerate(row):
            df_atmosphere.loc[i, val] = True

    df_atmosphere.columns = ["atmos_" + c for c in df_atmosphere.columns]
    union = df_atmosphere.reset_index().merge(union, on="index")

    df_atributos['Crowd'] = df_atributos['Crowd'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_crowd = df_atributos['Crowd'].explode().unique()[1:].tolist()

    df_crowd = pd.DataFrame([[False] * len(columnas_crowd)] * df_atributos.shape[0], columns=columnas_crowd)

    for i, row in enumerate(df_atributos['Crowd']):
        for j, val in enumerate(row):
            df_crowd.loc[i, val] = True

    df_crowd.columns = ["crowd_" + c for c in df_crowd.columns]
    union = df_crowd.reset_index().merge(union, on="index")       

    df_atributos['Dining options'] = df_atributos['Dining options'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_dining = df_atributos['Dining options'].explode().unique()[1:].tolist()
    df_dining = pd.DataFrame([[False] * len(columnas_dining)] * df_atributos.shape[0], columns=columnas_dining)

    for i, row in enumerate(df_atributos['Dining options']):
        for j, val in enumerate(row):
            df_dining.loc[i, val] = True

    df_dining.columns = ["dining_" + c for c in df_dining.columns]
    union = df_dining.reset_index().merge(union, on="index")

    df_atributos['Health & safety'] = df_atributos['Health & safety'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_health = df_atributos['Health & safety'].explode().unique()[1:].tolist()

    df_health = pd.DataFrame([[False] * len(columnas_health)] * df_atributos.shape[0], columns=columnas_health)

    for i, row in enumerate(df_atributos['Health & safety']):
        for j, val in enumerate(row):
            df_health.loc[i, val] = True

    df_health.columns = ["health_" + c for c in df_health.columns]
    union = df_health.reset_index().merge(union, on="index")   

    df_atributos['Highlights'] = df_atributos['Highlights'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_high = df_atributos['Highlights'].explode().unique()[1:].tolist()

    df_high = pd.DataFrame([[False] * len(columnas_high)] * df_atributos.shape[0], columns=columnas_high)

    for i, row in enumerate(df_atributos['Highlights']):
        for j, val in enumerate(row):
            df_high.loc[i, val] = True

    df_high.columns = ["high_" + c for c in df_high.columns]
    union = df_high.reset_index().merge(union, on="index")

    df_atributos['Offerings'] = df_atributos['Offerings'].apply(lambda r: r.replace("Kids'", "Kids"))

    df_atributos['Offerings'] = df_atributos['Offerings'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_offerings = df_atributos['Offerings'].explode().unique()[1:].tolist()

    df_offering = pd.DataFrame([[False] * len(columnas_offerings)] * df_atributos.shape[0], columns=columnas_offerings)

    for i, row in enumerate(df_atributos['Offerings']):
        for j, val in enumerate(row):
            df_offering.loc[i, val] = True

    df_offering.columns = ["offer_" + c for c in df_offering.columns]
    union = df_offering.reset_index().merge(union, on="index")

    df_atributos['Payments'] = df_atributos['Payments'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_payments = df_atributos['Payments'].explode().unique()[1:].tolist()

    df_payments = pd.DataFrame([[False] * len(columnas_payments)] * df_atributos.shape[0], columns=columnas_payments)

    for i, row in enumerate(df_atributos['Payments']):
        for j, val in enumerate(row):
            df_payments.loc[i, val] = True

    df_payments.columns = ["pay_" + c for c in df_payments.columns]
    union = df_payments.reset_index().merge(union, on="index")

    df_atributos['Popular for'] = df_atributos['Popular for'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_popular = df_atributos['Popular for'].explode().unique()[1:].tolist()

    df_popular = pd.DataFrame([[False] * len(columnas_popular)] * df_atributos.shape[0], columns=columnas_popular)

    for i, row in enumerate(df_atributos['Popular for']):
        for j, val in enumerate(row):
            df_popular.loc[i, val] = True

    df_popular.columns = ["popular_" + c for c in df_popular.columns]
    union = df_popular.reset_index().merge(union, on="index")

    df_atributos['Service options'] = df_atributos['Service options'].apply(lambda s: re.sub(r"'", "\"", s)).apply(lambda s: re.sub(r'"\s*"','","', s)).apply(json.loads)

    columnas_service = df_atributos['Service options'].explode().unique()[1:].tolist()

    df_service = pd.DataFrame([[False] * len(columnas_service)] * df_atributos.shape[0], columns=columnas_service)

    for i, row in enumerate(df_atributos['Service options']):
        for j, val in enumerate(row):
            df_service.loc[i, val] = True

    df_service.columns = ["service_" + c for c in df_service.columns]

    union = df_service.reset_index().merge(union, on="index")

    subset = [col for col in union.columns if '_' in col and col not in ['id_restaurante', 'id_estado', 'cantidad_resenas'] ]

    df = pd.DataFrame(data=None)

    # Accesibilidad
    df['index'] = union.index
    df['access_Wheelchair accessible entrance'] = union['access_Wheelchair accessible entrance'] | union['access_Wheelchair-accessible entrance']
    df['access_Wheelchair accessible parking lot'] = union['access_Wheelchair accessible parking lot'] | union['access_Wheelchair-accessible car park']
    df['access_Wheelchair accessible restroom'] = union['access_Wheelchair accessible restroom'] | union['access_Wheelchair-accessible toilet']
    df['access_Wheelchair accessible seating'] = union['access_Wheelchair accessible seating'] | union['access_Wheelchair-accessible seating']
    df['access_Wheelchair accessible elevator'] = union['access_Wheelchair accessible elevator'] | union['access_Wheelchair-accessible lift']

    # Amenities
    df['amen_Bar onsite'] = union['amen_Bar onsite'] | union['amen_Bar on site']
    df['amen_Gender-neutral restroom'] = union['amen_Gender-neutral restroom'] | union['amen_Gender-neutral toilets']
    df['amen_Good for kids'] = union['amen_Good for kids']
    df['amen_High chairs'] = union['amen_High chairs']
    df['amen_Restroom'] = union['amen_Restroom'] | union['amen_Toilets'] | union['amen_Public restroom']
    df['amen_Wi-Fi'] = union['amen_Wi-Fi']

    # Atmosphere
    df['atmos_Cozy'] = union['atmos_Cozy'] | union['atmos_Cosy']
    df['atmos_Historic'] = union['atmos_Historic']
    df['atmos_Romantic'] = union['atmos_Romantic']
    df['atmos_Upscale'] = union['atmos_Upscale']

    # Crowd
    df['crowd_College students'] = union['crowd_College students']
    df['crowd_Family-friendly'] = union['crowd_Family-friendly'] | union['crowd_Family friendly']
    df['crowd_Groups'] = union['crowd_Groups']
    df['crowd_Locals'] = union['crowd_Locals']
    df['crowd_Tourists'] = union['crowd_Tourists']
    df['crowd_University students'] = union['crowd_University students']

    # Dining
    df['dining_Breakfast'] = union['dining_Breakfast']
    df['dining_Catering'] = union['dining_Catering']
    df['dining_Dessert'] = union['dining_Dessert']
    df['dining_Dinner'] = union['dining_Dinner']
    df['dining_Lunch'] = union['dining_Lunch']
    df['dining_Seating'] = union['dining_Seating']

    # Health
    df['health_Mask required'] = union['health_Mask required']
    df['health_Reservations required'] = union['health_Reservations required']
    df['health_Safety dividers at checkout'] = union['health_Safety dividers at checkout']
    df['health_Staff get temperature checks'] = union['health_Staff get temperature checks']
    df['health_Staff required to disinfect surfaces between visits'] = union['health_Staff required to disinfect surfaces between visits']
    df['health_Staff wear masks'] = union['health_Staff wear masks']
    df['health_Temperature check required'] = union['health_Temperature check required']

    # High

    df['high_Fast service'] = union['high_Fast service']
    df['high_Great dessert'] = union['high_Great dessert'] | union['high_Great tea selection']
    df['high_LGBTQ friendly'] = union['high_LGBTQ friendly'] | union['high_LGBTQ-friendly'] | union['high_Transgender safespace']
    df['high_Live music'] = union['high_Live music'] | union['high_Bar games'] | union['high_Fireplace']
    df['high_Sports'] = union['high_Sports'] | union['high_Sport']

    # Offer
    df['offer_Alcohol'] = union['offer_Alcohol'] | union['offer_Hard liquor'] | union['offer_Spirits']
    df['offer_Beer'] = union['offer_Beer'] | union['high_Great beer selection']
    df['offer_Cocktails'] = union['offer_Cocktails'] | union['high_Great cocktails']
    df['offer_Coffee'] = union['offer_Coffee'] | union['high_Great coffee']
    df['offer_Comfort food'] = union['offer_Comfort food'] | union['offer_Prepared foods'] | union['offer_Small plates']
    df['offer_Food'] = union['offer_Food'] | union['offer_Food at bar'] | union['offer_All you can eat']
    df['offer_Halal food'] = union['offer_Halal food']
    df['offer_Happy hour drinks'] = union['offer_Happy hour drinks'] | union['offer_Happy-hour drinks']
    df['offer_Happy hour food'] = union['offer_Happy hour food'] | union['offer_Happy-hour food']
    df['offer_Healthy options'] = union['offer_Healthy options'] | union['offer_Organic dishes']
    df['offer_Kids menu'] = union['offer_Kids menu']
    df['offer_Quick bite'] = union['offer_Quick bite'] | union['offer_Late-night food']
    df['offer_Vegetarian options'] = union['offer_Vegetarian options'] | union['offer_Salad bar']
    df['offer_Wine'] = union['offer_Wine'] | union['high_Great wine list']

    # Payments

    df['pay_Cash-only'] = union['pay_Cash-only']
    df['pay_Checks'] = union['pay_Checks']
    df['pay_Credit cards'] = union['pay_Credit cards']
    df['pay_Debit cards'] = union['pay_Debit cards']
    df['pay_NFC mobile payments'] = union['pay_NFC mobile payments']

    # Popular

    df['popular_Breakfast'] = union['popular_Breakfast']
    df['popular_Dinner'] = union['popular_Dinner']
    df['popular_Good for working on laptop'] = union['popular_Good for working on laptop']
    df['popular_Lunch'] = union['popular_Lunch']
    df['popular_Solo dining'] = union['popular_Solo dining']

    # Service

    df['service_Curbside pickup'] = union['service_Curbside pickup'] | union['service_Drive-through']
    df['service_Delivery'] = union['service_Delivery'] | union['service_No-contact delivery']
    df['service_Dine-in'] = union['service_Dine-in']
    df['service_In-store shopping'] = union['service_In-store shopping'] | union['service_In-store pickup']
    df['service_Outdoor seating'] = union['service_Outdoor seating']
    df['service_Takeout'] = union['service_Takeout'] | union['service_Takeaway']

    return {'union':union, 'df':df}
#### Transformación

df_restaurantes = pd.read_parquet(os.path.join(BASE_PATH, SITIOS_FILE_URL))

def preprocess_data(data_frame):
    df = preprocess(data_frame)['df']
    union = preprocess(data_frame)['union']

    tasa = pd.Series(df.sum()/len(union))
    columnas = tasa[tasa >=0.005].index.tolist()
    print(f"Columnas de atributos: {len(columnas)} columnas resultantes")

    columnas_restaurante = pd.Series(union.columns).reset_index().loc[159:,0].tolist()
    print(f"Columnas de restaurantes: {len(columnas_restaurante)} columnas de datos restaurantes")

    # # df.to_csv("df_salida_preprocs_py.csv")
    # # union.to_csv("union_salida_preprocs_py.csv")


    df = pd.concat([df, union[columnas_restaurante]], axis=1)

    drop_col = ['id_estado','categorias','cantidad_resenas','latitud','longitud','atributos','calificacion','origen']

    # df.query("nombre == 'Subway'").drop(columns=drop_col).to_csv("preprocess_subway.csv")
    # print(df.sample(3))

    encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
    # enc.fit(range(1,51).reshape(-1,1))
    # enc.fit(df_restaurantes['id_estado'].reset_index())

    one_hot_encoded = encoder.fit_transform(df[['id_estado']].astype(int))
    # one_hot_encoded
    df_one_hot_encoded = pd.DataFrame(one_hot_encoded, columns=encoder.get_feature_names_out())
    # df[['id_estado']]

    df = pd.concat([df, df_one_hot_encoded], axis=1)

    us = union.query("nombre == 'Subway'")

    print(us.query("id_restaurante == '0x865681564f2dfd47:0x1f030438f1ceed23'"))

    # Definir las categorías y las correspondientes subcadenas de corte
    categorias = {
        'service': 8,
        'access': 7,
        'amen': 5,
        'atmos': 6,
        'crowd': 6,
        'dining': 7,
        'health': 7,
        'high': 5,
        'offer': 6,
        'pay': 4,
        'popular': 8
    }

    # # # Filtrar columnas por cada categoría
    # columnas = {cat: [col for col in us.columns if col.startswith(cat)] for cat in categorias.keys()}

    # chunk_size = 500
    # queries = []
    # id_counter = 1  # Asegúrate de inicializar el contador

    # with open('us_inserts_atributos.sql', 'w') as f:
    #     for cat, substr_len in categorias.items():
    #         for i, row in us.iterrows():
    #             for col in columnas[cat]:
    #                 if us.loc[i, col] == 1:
    #                     query = f"({id_counter}, '{us.loc[i,'id_restaurante']}', '{cat}', '{col[substr_len:]}')"
    #                     queries.append(query)
    #                     id_counter += 1

    #                     # Escribir al archivo en chunks de 100 consultas
    #                     if len(queries) >= chunk_size:
    #                         f.write("INSERT INTO atributos VALUES " + ", ".join(queries) + ";\n")
    #                         queries = []  # Vaciar la lista de consultas

    #     # Escribir cualquier consulta restante en la lista
    #     if queries:
    #         f.write("INSERT INTO atributos VALUES " + ", ".join(queries) + ";\n")

    df.to_csv("df.csv")

    fig = sns.boxplot(df_restaurantes['calificacion'])
    fig.plot()

    umbral = df['calificacion'].mean()
    df.loc[:, 'calificacion'] = df.loc[:, 'calificacion'].apply(lambda x: 1 if x >= umbral else 0)
    df.loc[:, 'calificacion'] = df.loc[:, 'calificacion'].astype(int)

    dropear = ["nombre","index", "id_restaurante","categorias", "id_estado", "atributos", "cantidad_resenas"]
    # dropear = ["index", "id_restaurante","categorias", "atributos", "nombre"]

    df = df.loc[:, ~df.columns.isin(dropear)]
    df = pd.concat([df.query("calificacion == 1").sample(48943),df.query("calificacion == 0")])
    df[df.columns[:-53]] = df[df.columns[:-53]].astype(int)

    scores = dict()

    X = df.drop(['calificacion'], axis=1)
    y = df['calificacion']

    return X, y

X, y = preprocess_data(df_restaurantes)

X.to_csv("X.csv")
y.to_csv("y.csv")

# X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)

# # scalers = [StandardScaler(), MaxAbsScaler(), MinMaxScaler()]
# # classifiers = [RandomForestClassifier(), LogisticRegression(), HistGradientBoostingClassifier(max_iter=100, learning_rate=0.1),
# #     XGBClassifier(use_label_encoder=False, eval_metric='logloss'), KNeighborsClassifier(), GradientBoostingClassifier()]

# # for s in scalers:
# #     for c in classifiers:
# #         print(f"Scaler: {s.__class__.__name__}, Classifier: {c.__class__.__name__}")
# #         pipe = Pipeline([('scaler', s), ('clasiffier', c)], verbose=True)
# #         pipe.fit(X_train, y_train)  # apply scaling on training data
# #         # scores["HistGradientBoostingClassifier"] = pipe.score(X_test, y_test)
# #         # Print the accuracy score
# #         score = pipe.score(X_test, y_test)
# #         print(f"Accuracy: {score:.4f}")

# #         if score >= 0.745:
# #             cm = confusion_matrix(y_test, pipe.predict(X_test))
# #             disp = ConfusionMatrixDisplay(confusion_matrix=cm)
# #             disp.plot(cmap='Blues')
# #             # disp = ConfusionMatrixDisplay.from_estimator(c, X_test, y_test, cmap='viridis', display_labels=pipe.classes_)
# #             # disp.plot()
# #             plt.title(f"Confusion Matrix - {s.__class__.__name__} + {c.__class__.__name__}")
# #             plt.show()
# # print(classification_report(y_test, pipe.predict(X_test)))



# # y_pred = loaded_pipeline.predict(X_test)
# # accuracy = accuracy_score(y_test, y_pred)
# # print(f"Test accuracy: {accuracy:.2f}")