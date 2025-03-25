from fastapi import FastAPI
from pydantic import BaseModel
import xgboost as xgb
import joblib
import numpy as np

# Initialize FastAPI app
app = FastAPI()

# Load Pretrained Models
eta_model = joblib.load("xgboost_model_pickup.pkl")
delay_model = joblib.load("logistic_regression_pickup.pkl")  # Logistic Regression Model

# Define feature columns
feature_cols = ["speed_kmh", "city_order_count", "pickup_time_delay", "pickup_distance_km",
                "avg_pickup_time_minutes", "month", "cluster", "day_of_week",
                "pickup_order_count", "speed_status_indexed"]

# Define request model using Pydantic
class PredictionRequest(BaseModel):
    speed_kmh: float
    city_order_count: int
    pickup_time_delay: float
    pickup_distance_km: float
    avg_pickup_time_minutes: float
    month: int
    cluster: int
    day_of_week: int
    pickup_order_count: int
    speed_status_indexed: int

@app.get("/")
def home():
    return {"message": "FastAPI is running!"}

@app.post("/predict")
async def predict(request: PredictionRequest):
    try:
        # Convert input to NumPy array
        features = np.array([[getattr(request, col) for col in feature_cols]])

        # Predict Delay (Logistic Regression)
        delay_prediction = delay_model.predict(features)[0]  # 0 or 1

        # Convert to DMatrix for XGBoost
        dmatrix_features = xgb.DMatrix(features)
        eta_prediction = eta_model.predict(dmatrix_features)[0]  # Continuous ETA value

        # Response
        return {
            "delay_prediction": int(delay_prediction),
            "eta_prediction": float(eta_prediction)
        }
    except Exception as e:
        return {"error": str(e)}

# Run FastAPI using: uvicorn api:app --host 0.0.0.0 --port 5000
