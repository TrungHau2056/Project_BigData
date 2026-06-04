import os
import logging
import pandas as pd
import xgboost as xgb
import mlflow
import mlflow.xgboost
import boto3
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score

os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("Ecommerce_XGBoost_Demo")

def main():
    logger.info("Connecting to MinIO via Python...")

    storage_options = {
        "client_kwargs": {"endpoint_url": "http://localhost:9000"},
        "key": "minioadmin",
        "secret": "minioadmin"
    }

    session_features_path = "s3://ecommerce-datalake/features/session_features/"
    logger.info(f"Loading data from: {session_features_path}")
    
    df = pd.read_parquet(session_features_path, storage_options=storage_options)
    
    logger.info(f"Loaded {len(df)} rows. Extracting 1000 rows for Demo...")
    demo_pd_df = df.head(1000)

    feature_cols = [
        'total_views', 'total_carts', 'unique_categories', 
        'unique_brands', 'session_duration', 'hour_of_day', 'cart_to_view_ratio'
    ]
    
    X = demo_pd_df[feature_cols]
    y = demo_pd_df['label']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logger.info("Starting MLflow run...")
    
    with mlflow.start_run(run_name="XGBoost_Demo_1k_Records"):
        
        params = {
            'objective': 'binary:logistic',
            'eval_metric': 'aucpr',
            'max_depth': 6,
            'learning_rate': 0.1,
            'scale_pos_weight': 45,
            'subsample': 0.8,
            'seed': 42
        }
        
        mlflow.log_params(params)
        
        logger.info("Training XGBoost model...")
        dtrain = xgb.DMatrix(X_train, label=y_train)
        dtest = xgb.DMatrix(X_test, label=y_test)
        
        bst = xgb.train(
            params, dtrain, 
            num_boost_round=100, 
            evals=[(dtest, 'eval')], 
            early_stopping_rounds=10,
            verbose_eval=False
        )

        logger.info("Calculating evaluation metrics...")
        y_pred_proba = bst.predict(dtest)
        
        roc_auc = roc_auc_score(y_test, y_pred_proba)
        pr_auc = average_precision_score(y_test, y_pred_proba)
        
        logger.info(f"Result -> ROC-AUC: {roc_auc:.4f} | PR-AUC: {pr_auc:.4f}")
        
        mlflow.log_metric("roc_auc", roc_auc)
        mlflow.log_metric("pr_auc", pr_auc)

        logger.info("Logging model to MLflow Registry...")
        mlflow.xgboost.log_model(bst, "xgboost-session-model")
        
        logger.info("Exporting model to MinIO models directory...")
        
        local_model_path = "xgboost_model.json"
        bst.save_model(local_model_path)
        
        s3_client = boto3.client(
            's3',
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin"
        )
        
        minio_bucket = "ecommerce-datalake"
        minio_key = "models/xgboost_session_model.json"
        
        s3_client.upload_file(local_model_path, minio_bucket, minio_key)
        
        logger.info(f"Model saved successfully at: s3://{minio_bucket}/{minio_key}")
        
        if os.path.exists(local_model_path):
            os.remove(local_model_path)

        logger.info("End-to-end demo training and model saving completed.")

if __name__ == "__main__":
    main()
