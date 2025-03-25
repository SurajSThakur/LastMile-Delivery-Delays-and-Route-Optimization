from flask import Flask, request, jsonify
import numpy as np
import joblib
import xgboost as xgb

# Load Pretrained Models
eta_model = joblib.load("xgboost_model_pickup.pkl")
delay_model = joblib.load("logistic_regression_pickup.pkl")

# Initialize Flask App
app = Flask(__name__)

# Feature columns (according to your pipeline)
feature_cols = ["speed_kmh", "city_order_count", "pickup_time_delay", "pickup_distance_km", 
                "avg_pickup_time_minutes", "month", "cluster", "day_of_week", 
                "pickup_order_count", "speed_status_indexed"]

@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get JSON Data
        data = request.get_json()
        
        # Convert input data to NumPy array (1 row, 10 features)
        features = np.array([[data[col] for col in feature_cols]])

        # Predict Delay (Logistic Regression)
        delay_prediction = delay_model.predict(features)[0]  # 0 or 1
        
        # ✅ Convert NumPy array to XGBoost DMatrix before passing to eta_model
        dmatrix_features = xgb.DMatrix(features)  
        eta_prediction = eta_model.predict(dmatrix_features)[0]  # Continuous ETA value
        
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