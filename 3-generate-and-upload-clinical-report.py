import os
import argparse
import random
import string
import requests
from datetime import datetime
from elasticsearch import Elasticsearch, helpers, NotFoundError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Elasticsearch connection details
CLOUD_ID = os.getenv('CLOUD_ID')
API_KEY = os.getenv('API_KEY')
INDEX_NAME = "notes-" + os.getenv('INDEX_NAME')
PIPELINE_NAME = "pipeline-" + INDEX_NAME

def debug_print(message, debug_mode):
    if debug_mode:
        print(f"DEBUG: {message}")

# Connect to Elasticsearch
def connect_to_elasticsearch(debug_mode):
    debug_print("Connecting to Elasticsearch...", debug_mode)
    try:
        es = Elasticsearch(cloud_id=CLOUD_ID, api_key=API_KEY)
        debug_print("Successfully connected to Elasticsearch", debug_mode)
        return es
    except Exception as e:
        debug_print(f"Failed to connect to Elasticsearch: {str(e)}", debug_mode)
        raise

es = None  # Global variable for Elasticsearch connection

def get_random_user(debug_mode):
    debug_print("Fetching random user data...", debug_mode)
    response = requests.get('https://randomuser.me/api/?nat=nz')
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        full_name = f"{user_data['name']['first']} {user_data['name']['last']}"
        dob = user_data['dob']['date'].split('T')[0]
        address = f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}, {user_data['location']['city']}, {user_data['location']['postcode']}, {user_data['location']['state']}, New Zealand"
        debug_print(f"Random user data fetched: {full_name}", debug_mode)
        return full_name, dob, address
    else:
        debug_print("Failed to fetch random user data, using default", debug_mode)
        return "John Doe", "1980-01-01", "123 Fake Street, Faketown, 0000, FakeState, New Zealand"

def generate_nhi(debug_mode):
    nhi = ''.join(random.choices(string.ascii_uppercase + string.digits, k=7))
    debug_print(f"Generated NHI: {nhi}", debug_mode)
    return nhi

def create_pipeline(debug_mode):
    debug_print("Creating pipeline...", debug_mode)
    pipeline_body = {
        "description": "Process clinical notes",
        "processors": [
            {
                "script": {
                    "source": """
                              ctx.clinical_data = 'Patient Name: ' + ctx.patient_name + ', ' +
                              'DOB: ' + ctx.dob + ', ' +
                              'NHI: ' + ctx.nhi + ', ' +
                              'GP Name: ' + ctx.gp_name + ', ' +
                              'Condition: ' + ctx.condition + ', ' +
                              'Gender: ' + ctx.gender + ', ' +
                              'Age: ' + ctx.age + ', ' +
                              'Note Date: ' + ctx.note_date + ', ' +
                              'Clinical Note: ' + ctx.clinical_note;
                    """
                }
            },
            {
                "inference": {
                    "model_id": ".elser_model_2",
                    "input_output": [ 
                     {
                            "input_field": "clinical_data",
                            "output_field": "text_embedding"
                      }
                     ]
                 }    
            },
            {
                "remove": {
                    "field": ["clinical_data"]
                }
            }
        ]
    }

    try:
        es.ingest.put_pipeline(id=PIPELINE_NAME, body=pipeline_body)
        debug_print(f"Pipeline '{PIPELINE_NAME}' created successfully", debug_mode)
    except Exception as e:
        debug_print(f"Error creating pipeline: {e}", debug_mode)
        raise

def create_index(debug_mode):
    debug_print("Creating index...", debug_mode)
    index_body = {
        "mappings": {
            "properties": {
                "patient_name": {"type": "text"},
                "dob": {"type": "date", "format": "yyyy-MM-dd"},
                "patient_address": {"type": "text"},
                "nhi": {"type": "keyword"},
                "gp_name": {"type": "text"},
                "condition": {"type": "keyword"},
                "gender": {"type": "keyword"},
                "age": {"type": "integer"},
                "note_date": {"type": "date", "format": "yyyy-MM-dd"},
                "clinical_note": {"type": "text"},
                "text_embedding": {"type": "sparse_vector"}
            }
        }
    }

    try:
        es.indices.create(index=INDEX_NAME, body=index_body)
        debug_print(f"Index '{INDEX_NAME}' created successfully", debug_mode)
    except Exception as e:
        debug_print(f"Error creating index: {e}", debug_mode)
        raise

def delete_index(debug_mode):
    debug_print(f"Deleting index '{INDEX_NAME}'...", debug_mode)
    try:
        es.indices.delete(index=INDEX_NAME)
        debug_print(f"Index '{INDEX_NAME}' deleted successfully", debug_mode)
    except NotFoundError:
        debug_print(f"Index '{INDEX_NAME}' not found", debug_mode)
    except Exception as e:
        debug_print(f"Error deleting index: {e}", debug_mode)
        raise

def delete_pipeline(debug_mode):
    debug_print(f"Deleting pipeline '{PIPELINE_NAME}'...", debug_mode)
    try:
        es.ingest.delete_pipeline(id=PIPELINE_NAME)
        debug_print(f"Pipeline '{PIPELINE_NAME}' deleted successfully", debug_mode)
    except NotFoundError:
        debug_print(f"Pipeline '{PIPELINE_NAME}' not found", debug_mode)
    except Exception as e:
        debug_print(f"Error deleting pipeline: {e}", debug_mode)
        raise

def index_exists(debug_mode):
    exists = es.indices.exists(index=INDEX_NAME)
    debug_print(f"Index '{INDEX_NAME}' exists: {exists}", debug_mode)
    return exists

def pipeline_exists(debug_mode):
    try:
        es.ingest.get_pipeline(id=PIPELINE_NAME)
        debug_print(f"Pipeline '{PIPELINE_NAME}' exists", debug_mode)
        return True
    except NotFoundError:
        debug_print(f"Pipeline '{PIPELINE_NAME}' does not exist", debug_mode)
        return False
    except Exception as e:
        debug_print(f"Error checking pipeline: {e}", debug_mode)
        raise

def read_csv_data(csv_file, debug_mode):
    debug_print(f"Reading CSV file: {csv_file}", debug_mode)
    import csv
    data = {}
    try:
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                condition = row['Condition']
                if condition not in data:
                    data[condition] = {
                        'Gender': row['Gender'],
                        'Age': row['Age'],
                        'Visits': []
                    }
                data[condition]['Visits'].append((row['Date'], row['Note']))
        
        # Sort visits by date for each condition
        for condition in data:
            data[condition]['Visits'].sort(key=lambda x: datetime.strptime(x[0], '%Y-%m-%d'))
        
        debug_print(f"Successfully read {len(data)} conditions from CSV", debug_mode)
        return data
    except Exception as e:
        debug_print(f"Error reading CSV file: {e}", debug_mode)
        raise

def generate_and_upload_data(input_csv, simulate, debug_mode):
    debug_print("Generating and uploading data...", debug_mode)
    data = read_csv_data(input_csv, debug_mode)

    total_documents = 0
    failed_documents = 0
    used_notes = {}

    for condition, patient_data in data.items():
        patient_name, patient_dob, patient_address = get_random_user(debug_mode)
        gp_name, _, _ = get_random_user(debug_mode)
        nhi = generate_nhi(debug_mode)

        patient_key = f"{patient_name}_{nhi}"
        used_notes[patient_key] = set()

        actions = []
        for visit_date, clinical_note in patient_data['Visits']:
            # Create a unique key for this note
            note_key = f"{condition}_{clinical_note}"
            
            # If this note has been used for this patient, modify it
            count = 1
            while note_key in used_notes[patient_key]:
                clinical_note = f"{clinical_note} (Variation {count})"
                note_key = f"{condition}_{clinical_note}"
                count += 1
            
            used_notes[patient_key].add(note_key)

            action = {
                "_index": INDEX_NAME,
                "_source": {
                    "patient_name": patient_name,
                    "dob": patient_dob,
                    "patient_address": patient_address,
                    "nhi": nhi,
                    "gp_name": f"Dr. {gp_name}",
                    "condition": condition,
                    "gender": patient_data['Gender'],
                    "age": int(patient_data['Age']),
                    "note_date": visit_date,
                    "clinical_note": clinical_note
                }
            }
            actions.append(action)

        debug_print(f"Generated {len(actions)} actions for condition: {condition}", debug_mode)

        if simulate:
            print(f"Simulation mode: Not uploading to Elasticsearch. Sample action: {actions[0]}")
            return

        try:
            success, failed = helpers.bulk(es, actions, pipeline=PIPELINE_NAME, stats_only=True)
            total_documents += success
            failed_documents += failed
            debug_print(f"Uploaded {success} documents for condition: {condition}. Failed: {failed}", debug_mode)
        except helpers.BulkIndexError as e:
            debug_print(f"Bulk indexing failed for condition {condition}: {e}", debug_mode)
            failed_documents += len(actions)
        except Exception as e:
            debug_print(f"Unexpected error occurred while uploading condition {condition}: {e}", debug_mode)
            failed_documents += len(actions)

    print(f"Total documents uploaded: {total_documents}")
    print(f"Total documents failed: {failed_documents}")

def main(args):
    global es
    try:
        es = connect_to_elasticsearch(args.debug)
        
        if args.create_pipeline:
            create_pipeline(args.debug)
        elif args.create_index:
            if not index_exists(args.debug):
                create_index(args.debug)
            else:
                debug_print(f"Index '{INDEX_NAME}' already exists", args.debug)
        elif args.delete_index:
            delete_index(args.debug)
        elif args.delete_pipeline:
            delete_pipeline(args.debug)
        elif args.input_csv:
            if not pipeline_exists(args.debug):
                debug_print(f"Pipeline '{PIPELINE_NAME}' does not exist. Please create it first using --create-pipeline", args.debug)
                exit(1)
            if not index_exists(args.debug):
                debug_print(f"Index '{INDEX_NAME}' does not exist. Please create it first using --create-index", args.debug)
                exit(1)
            generate_and_upload_data(args.input_csv, args.simulate, args.debug)
        else:
            debug_print("Please specify either --create-pipeline, --create-index, --delete-index, --delete-pipeline, or --input-csv", args.debug)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        if args.debug:
            import traceback
            print(traceback.format_exc())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Elasticsearch index and ingest clinical notes")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create-pipeline", action="store_true", help="Create the ingest pipeline")
    group.add_argument("--create-index", action="store_true", help="Create the Elasticsearch index")
    group.add_argument("--delete-index", action="store_true", help="Delete the Elasticsearch index")
    group.add_argument("--delete-pipeline", action="store_true", help="Delete the ingest pipeline")
    group.add_argument("--input-csv", type=str, help="Input CSV file containing symptom data")
    parser.add_argument("--simulate", action="store_true", help="Simulate data generation without uploading to Elasticsearch")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    args = parser.parse_args()

    main(args)
