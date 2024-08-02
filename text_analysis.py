from elasticsearch.exceptions import RequestError
import os, re, json, requests
from dotenv import load_dotenv

load_dotenv()

ELASTIC_URL = os.getenv("ELASTIC_URL")
API_KEY = os.getenv("API_KEY")

def perform_text_analysis(analysis_type, text, es, model):
    if analysis_type == "Named Entity Recognition":
        return named_entity_recognition(text, model)
    elif analysis_type == "Sentiment Analysis":
        return sentiment_analysis(text, model)
    elif analysis_type == "Zero Shot Recognition":
        return zero_shot_recognition(text, model)
    else:
        return "Invalid analysis type"

def named_entity_recognition(text, ner_model):
    try:
        result = infer_zeroshot(ELASTIC_URL, API_KEY, ner_model, text)
        predicted_value = result.get("predicted_value", "")
        
        # Replace entity tags with icons, removing brackets and repeated text
        predicted_value = re.sub(r'\[([^\]]+)\]\(PER&[^)]+\)', r'\1 👤', predicted_value)
        predicted_value = re.sub(r'\[([^\]]+)\]\(LOC&[^)]+\)', r'\1 🌎', predicted_value)
        predicted_value = re.sub(r'\[([^\]]+)\]\(ORG&[^)]+\)', r'\1 🏢', predicted_value)
        
        formatted_result = f"Named Entities:\n{predicted_value}"
        return formatted_result
    except Exception as e:
        return f"Error performing Named Entity Recognition: {str(e)}"

def infer_zeroshot(elastic_url, api_key, model_id, input_text, labels=None):
    endpoint = f"{elastic_url}/_ml/trained_models/{model_id}/_infer"
    payload = {
        "docs": [
            {
                "text_field": input_text
            }
        ]
    }
    if labels:
        payload["inference_config"] = {
            "classification": {
                "num_top_classes": len(labels),
                "labels": labels
            }
        }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"ApiKey {api_key}"
    }
    response = requests.post(endpoint, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()["inference_results"][0]

def sentiment_analysis(text, sentiment_model):
    try:
        result = infer_zeroshot(ELASTIC_URL, API_KEY, sentiment_model, text)
        predicted_label = result.get("predicted_value")
        prediction_probability = result.get("prediction_probability", 0)
        
        sentiment_emoji = {
            "positive": "😊",
            "negative": "😞",
            "neutral": "😐"
        }
        
        emoji = sentiment_emoji.get(predicted_label.lower(), "")
        
        formatted_result = f"Sentiment: {predicted_label} {emoji}\n"
        formatted_result += f"Confidence: {prediction_probability:.2f}"
        return formatted_result
    except Exception as e:
        return f"Error performing Sentiment Analysis: {str(e)}"

def zero_shot_recognition(text, zero_shot_model):
    categories = ["Healthcare", "Technology", "Finance", "Education"]
    try:
        result = infer_zeroshot(ELASTIC_URL, API_KEY, zero_shot_model, text, categories)
        predictions = result.get("prediction", [])
        
        # Sort predictions by score in descending order
        sorted_predictions = sorted(predictions, key=lambda x: x['score'], reverse=True)
        
        formatted_result = "Zero-shot Classification Results:\n"
        for pred in sorted_predictions:
            formatted_result += f"{pred['label']}: {pred['score']:.2f}\n"
        
        return formatted_result
    except Exception as e:
        return f"Error performing Zero Shot Recognition: {str(e)}"
