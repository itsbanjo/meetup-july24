import os
import argparse
from elasticsearch import Elasticsearch, NotFoundError
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
        "type": "text_expansion"
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
        "type": "zero_shot_classification"
    }
}

def is_model_installed(model_name):
    try:
        es.ml.get_trained_models(model_id=model_name)
        return True
    except NotFoundError:
        return False

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

def check_and_deploy_models():
    print("Checking model status and deploying missing models...")
    for model_key, model in models.items():
        if not is_model_installed(model["name"]):
            print(f"{model_key.upper()} model is not installed. Deploying...")
            deploy_model(model_key)
        else:
            print(f"{model_key.upper()} model is already installed.")

def check_models():
    print("Checking model status...")
    installed = []
    not_installed = []
    for model_key, model in models.items():
        if is_model_installed(model["name"]):
            installed.append(model_key)
            print(f"{model_key.upper()} model is installed.")
        else:
            not_installed.append(model_key)
            print(f"{model_key.upper()} model is not installed.")
    
    print("\nSummary:")
    if installed:
        print("Installed models:")
        for model in installed:
            print(f"- {model.upper()}")
    if not_installed:
        print("Models not installed:")
        for model in not_installed:
            print(f"- {model.upper()}")
    else:
        print("All models are installed.")

def main():
    parser = argparse.ArgumentParser(description="Manage ML models in Elasticsearch")
    parser.add_argument("--deploy", action="store_true", help="Check and deploy missing models")
    parser.add_argument("--remove", action="store_true", help="Remove all models")
    parser.add_argument("--check", action="store_true", help="Check which models are installed")
    args = parser.parse_args()

    if sum([args.deploy, args.remove, args.check]) != 1:
        print("Error: Please specify exactly one of --deploy, --remove, or --check flags.")
        return

    if args.deploy:
        check_and_deploy_models()
    elif args.remove:
        for model in models:
            remove_model(model)
    elif args.check:
        check_models()

if __name__ == "__main__":
    main()
