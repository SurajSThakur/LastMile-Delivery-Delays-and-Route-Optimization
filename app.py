from flask import Flask, request, jsonify
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import pandas as pd
from xgboost import XGBRegressor
import joblib
import numpy as np

# Initialize Spark Session
spark = SparkSession.builder.appName("ModelAPI").getOrCreate()

# Load Pretrained Models
eta_model = joblib.load("eta_model_pickup.pkl")
delay_model = joblib.load("delay_model_pickup.pkl")
# Initialize Flask App
app = Flask(__name__)

# Feature columns (adjust according to your pipeline)
feature_cols = ["speed_status_indexed", "pca1", "pca2", "pca3", "pca4", "pca5", "pca6", "pca7", "pca8", "pca9","pca10"]

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get JSON Data
        data = request.get_json()
        
        # Extract Features
        features = np.array([
            data["speed_status_indexed"],
            data["pca1"], data["pca2"], data["pca3"], data["pca4"], 
            data["pca5"], data["pca6"], data["pca7"], data["pca8"], data["pca9"], data["pca10"]
        ]).reshape(1, -1)  # Reshape for model

        # Predict Delay Probability (Logistic Regression)
        delay_prediction = delay_model.predict(features)[0]  # 0 or 1
        
        # Predict ETA (XGBoost)
        eta_prediction = eta_model.predict(features)[0]  # Continuous value
        
        # Response
        response = {
            "delay_prediction": int(delay_prediction),  # Convert to int
            "eta_prediction": float(eta_prediction)  # Convert to float
        }
        return jsonify(response)
    
    except Exception as e:
        return jsonify({"error": str(e)})

# Run Flask App
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)