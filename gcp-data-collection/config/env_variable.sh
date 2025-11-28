export GCP_PROJECT_ID=$(gcloud config get-value project)
export GCP_REGION=$(gcloud config get-value compute/region)
export GCP_ZONE=$(gcloud config get-value compute/zone)
export GCS_RAW_BUCKET="raw_glamira_data"
export GCP_SERVICE_ACCOUNT_KEY="$PWD/credentials/de-k19-key.json" 
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="countly"