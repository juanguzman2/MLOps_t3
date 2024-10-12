from airflow.decorators import task, dag
from datetime import datetime, timedelta


@task.virtualenv(
    task_id="prediction_model", 
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn",
        "joblib"
    ], 
    system_site_packages=False
)
def task_predictionnmodel():
    import sys
    from joblib import load
    import pandas as pd

    PATH_COMMON = '../'
    sys.path.append(PATH_COMMON)

    # Cargar datos y el modelo guardado
    X_train = pd.read_csv('/opt/airflow/dags/data/inputs/xtrain.csv')
    classifier = load("/opt/airflow/dags/data/model/best_random_forest_model.joblib")

    # Seleccionar características
    features = pd.read_csv('/opt/airflow/dags/data/inputs/selected_features.csv')
    features = features['0'].to_list()
    X_train = X_train[features]

    # Realizar predicciones
    predictions = classifier.predict(X_train)
    predictions_df = pd.DataFrame(predictions, columns=['prediction'])

    # Guardar predicciones en un archivo CSV
    predictions_df.to_csv('/opt/airflow/dags/data/outputs/predictions.csv', index=False)
    
    # Convertir las predicciones a una cadena para enviarlas por correo
    predictions_str = predictions_df.to_string(index=False)
    
    print("Predicción Generada")
    return predictions_str  # Devuelve las predicciones como cadena




# Tarea para monitorear el modelo
@task.virtualenv(
    task_id="monitoring_model", 
    requirements=[
        "pandas", 
        "numpy", 
        "scikit-learn",
        "joblib"
        ], 
    system_site_packages=False
)
def task_monitoringmodel():
    import pandas as pd
    import numpy as np
    from joblib import load
    from sklearn.metrics import mean_absolute_percentage_error, r2_score
    import os

    # Cargar los datos
    X_train = pd.read_csv('/opt/airflow/dags/data/inputs/xtrain.csv')
    y_train = pd.read_csv('/opt/airflow/dags/data/inputs/ytrain.csv')

    X_test = pd.read_csv('/opt/airflow/dags/data/inputs/xtest.csv')
    y_test = pd.read_csv('/opt/airflow/dags/data/inputs/ytest.csv')

    # Cargar el modelo
    lin_model = load("/opt/airflow/dags/data/model/best_random_forest_model.joblib")

    # Cargar las características seleccionadas
    features = pd.read_csv('/opt/airflow/dags/data/inputs/selected_features.csv')
    features = features['0'].to_list()
    X_train = X_train[features]
    X_test = X_test[features]

    # Hacer predicciones y calcular métricas
    pred_train = lin_model.predict(X_train)
    pred_test = lin_model.predict(X_test)


    train_mse = mean_absolute_percentage_error(y_train, pred_train)
    train_r2 = r2_score(y_train, pred_train)

    test_mape = mean_absolute_percentage_error(y_test, pred_test)
    test_r2 = r2_score(y_test, pred_test)

    # Guardar resultados
    metrics_results = pd.DataFrame({
        'Train_MSE': [train_mse],
        'Train_R2': [train_r2],
        'test_mape': [test_mape],
        'Test_R2': [test_r2]
    })
    metrics_results.to_csv('/opt/airflow/dags/data/outputs/metrics_results.csv', index=False)

    print("Monitoreo Completado")

