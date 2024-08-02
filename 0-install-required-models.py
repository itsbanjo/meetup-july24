import os
import argparse
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Elasticsearch client setup
es = Elasticsearch(
    cloud_id=os.getenv("CLOUD_ID"),
    api_key=os.getenv("API_KEY")
)

# Model configurations
models = {
    "elser": {
        "name": os.getenv("ELSER_MODEL"),
        "type": "sparse_encoding"
    },
    "ner": {
        "name": os.getenv("NER_MODEL"),
        "type": "ner"
    },
    "sentiment": {
        "name": os.getenv("SENTIMENT_MODEL"),
        "type": "text_classification"
    },
    "zero_shot": {
        "name": os.getenv("ZERO_SHOT_MODEL"),
        "type": "text_classification"
    }
}

def deploy_model(model_key):
    model = models[model_key]
    print(f"Deploying {model_key.upper()} model...")
    try:
        es.ml.put_trained_model(
            model_id=model["name"],
            inference_config={model["type"]: {}},
            input={"field_names": ["text_field"]},
            description=f"{model_key.upper()} model"
        )
        es.ml.start_trained_model_deployment(model_id=model["name"])
        print(f"{model_key.upper()} model deployed successfully.")
    except Exception as e:
        print(f"Error deploying {model_key.upper()} model: {str(e)}")

def remove_model(model_key):
    model = models[model_key]
    print(f"Removing {model_key.upper()} model...")
    try:
        es.ml.stop_trained_model_deployment(model_id=model["name"])
        es.ml.delete_trained_model(model_id=model["name"])
        print(f"{model_key.upper()} model removed successfully.")
    except Exception as e:
        print(f"Error removing {model_key.upper()} model: {str(e)}")

def check_models():
    print("Checking model status...")
    not_installed = []
    for model_key, model in models.items():
        try:
            es.ml.get_trained_models(model_id=model["name"])
            print(f"{model_key.upper()} model is installed.")
        except Exception:
            print(f"{model_key.upper()} model is not installed.")
            not_installed.append(model_key)
    
    if not_installed:
        print("\nModels not installed:")
        for model in not_installed:
            print(f"- {model.upper()}")
    else:
        print("\nAll models are installed.")

def main():
    parser = argparse.ArgumentParser(description="Manage ML models in Elasticsearch")
    parser.add_argument("--deploy", action="store_true", help="Deploy the models")
    parser.add_argument("--remove", action="store_true", help="Remove the models")
    parser.add_argument("--check", action="store_true", help="Check which models are not installed")
    args = parser.parse_args()

    if sum([args.deploy, args.remove, args.check]) != 1:
        print("Error: Please specify exactly one of --deploy, --remove, or --check flags.")
        return

    if args.deploy:
        for model in models:
            deploy_model(model)
    elif args.remove:
        for model in models:
            remove_model(model)
    elif args.check:
        check_models()

if __name__ == "__main__":
    main()
