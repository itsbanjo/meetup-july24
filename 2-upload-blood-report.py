import os
import base64
import glob
from elasticsearch import Elasticsearch, helpers, NotFoundError
from dotenv import load_dotenv
import argparse
import json
import time

# Load environment variables
load_dotenv()

# Elasticsearch connection details
CLOUD_ID = os.getenv('CLOUD_ID')
API_KEY = os.getenv('API_KEY')
INDEX_NAME = os.getenv('INDEX_NAME')
PIPELINE_NAME = os.getenv('PIPELINE_NAME')

# Connect to Elasticsearch
es = Elasticsearch(cloud_id=CLOUD_ID, api_key=API_KEY)

def create_pipeline():
    pipeline_body = {
        "description": "Extract attachment information and specific fields",
        "processors": [
            {
                "attachment": {
                    "field": "data",
                    "target_field": "attachment",
                    "indexed_chars": -1
                }
            },
            {
                "grok": {
                    "field": "attachment.content",
                    "patterns": [
                        "Random Lab\\nPO Box \\d+, \\w+, \\w+ \\w+\\n\\nPatient: %{DATA:patient_name} NHI: %{DATA:nhi}\\n\\nAddress: %{DATA:address} Sex: %{WORD:sex}\\n\\nAge: %{NUMBER:age:int} years Date of birth: %{DATA:dob}\\n\\nLab: %{DATA:lab}\\n\\nBLOOD COUNT\\n\\nDate: %{DATA:test_date}\\nLab Numbers: %{DATA:lab_numbers}\\n\\nParameter Measurement Ref. Range\\n\\nHaemoglobin %{NUMBER:haemoglobin:float} g/L %{DATA:haemoglobin_range}\\n\\nRBC %{NUMBER:rbc:float} x10¹²/L %{DATA:rbc_range}\\n\\nHCT %{NUMBER:hct:float} N/A %{DATA:hct_range}\\n\\nMCV %{NUMBER:mcv:float} fL %{DATA:mcv_range}\\n\\nMCH %{NUMBER:mch:float} pg %{DATA:mch_range}\\n\\nPlatelets %{NUMBER:platelets:float} x10■/L %{DATA:platelets_range}\\n\\nWBC %{NUMBER:wbc:float} x10■/L %{DATA:wbc_range}\\n\\nNeutrophils %{NUMBER:neutrophils:float} x10■/L %{DATA:neutrophils_range}\\n\\nLymphocytes %{NUMBER:lymphocytes:float} x10■/L %{DATA:lymphocytes_range}\\n\\nMonoocytes %{NUMBER:monocytes:float} x10■/L %{DATA:monocytes_range}\\n\\nEosinophils %{NUMBER:eosinophils:float} x10■/L %{DATA:eosinophils_range}\\n\\nBasophils %{NUMBER:basophils:float} x10■/L %{DATA:basophils_range}"
                    ]
                }
            },
            {
                "script": {
                    "source": """
                              ctx.clinical_data = 'Patient Name: ' + ctx.patient_name + ', ' +
                              'NHI: ' + ctx.nhi + ', ' +
                              'Haemoglobin: ' + ctx.haemoglobin + ', ' +
                              'RBC: ' + ctx.rbc + ', ' +
                              'HCT: ' + ctx.hct + ', ' +
                              'MCV: ' + ctx.mcv + ', ' +
                              'MCH: ' + ctx.mch + ', ' +
                              'Platelets: ' + ctx.platelets + ', ' +
                              'WBC: ' + ctx.wbc + ', ' +
                              'Neutrophils: ' + ctx.neutrophils + ', ' +
                              'Lymphocytes: ' + ctx.lymphocytes + ', ' +
                              'Monoocytes: ' + ctx.monocytes + ', ' +
                              'Eosinophils: ' + ctx.eosinophils + ', ' +
                              'Basophils: ' + ctx.basophils;
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
                    "field": [ "data", "attachment.content", "clinical_data" ]
                }
            }
        ]
    }

    try:
        es.ingest.put_pipeline(id=PIPELINE_NAME, body=pipeline_body)
        print(f"Pipeline '{PIPELINE_NAME}' created successfully")
    except Exception as e:
        print(f"Error creating pipeline: {e}")
        exit(1)

def create_index():
    index_body = {
        "mappings": {
            "properties": {
                "patient_name": {"type": "text"},
                "nhi": {"type": "keyword"},
                "address": {"type": "text"},
                "sex": {"type": "keyword"},
                "age": {"type": "integer"},
                "dob": {"type": "date", "format": "yyyy-MM-dd"},
                "lab": {"type": "keyword"},
                "test_date": {"type": "date", "format": "dd/MM/yyyy"},
                "lab_numbers": {"type": "keyword"},
                "haemoglobin": {"type": "float"},
                "haemoglobin_range": {"type": "keyword"},
                "rbc": {"type": "float"},
                "rbc_range": {"type": "keyword"},
                "hct": {"type": "float"},
                "hct_range": {"type": "keyword"},
                "mcv": {"type": "float"},
                "mcv_range": {"type": "keyword"},
                "mch": {"type": "float"},
                "mch_range": {"type": "keyword"},
                "platelets": {"type": "float"},
                "platelets_range": {"type": "keyword"},
                "wbc": {"type": "float"},
                "wbc_range": {"type": "keyword"},
                "neutrophils": {"type": "float"},
                "neutrophils_range": {"type": "keyword"},
                "lymphocytes": {"type": "float"},
                "lymphocytes_range": {"type": "keyword"},
                "monocytes": {"type": "float"},
                "monocytes_range": {"type": "keyword"},
                "eosinophils": {"type": "float"},
                "eosinophils_range": {"type": "keyword"},
                "basophils": {"type": "float"},
                "basophils_range": {"type": "keyword"},
                "text_embedding": {"type": "sparse_vector" }
            }
        }
    }

    try:
        es.indices.create(index=INDEX_NAME, body=index_body)
        print(f"Index '{INDEX_NAME}' created successfully")
    except Exception as e:
        print(f"Error creating index: {e}")
        exit(1)

def delete_index():
    try:
        es.indices.delete(index=INDEX_NAME)
        print(f"Index '{INDEX_NAME}' deleted successfully")
    except NotFoundError:
        print(f"Index '{INDEX_NAME}' not found")
    except Exception as e:
        print(f"Error deleting index: {e}")
        exit(1)

def delete_pipeline():
    try:
        es.ingest.delete_pipeline(id=PIPELINE_NAME)
        print(f"Pipeline '{PIPELINE_NAME}' deleted successfully")
    except NotFoundError:
        print(f"Pipeline '{PIPELINE_NAME}' not found")
    except Exception as e:
        print(f"Error deleting pipeline: {e}")
        exit(1)

def index_exists():
    return es.indices.exists(index=INDEX_NAME)

def pipeline_exists():
    try:
        es.ingest.get_pipeline(id=PIPELINE_NAME)
        return True
    except NotFoundError:
        return False
    except Exception as e:
        print(f"Error checking pipeline: {e}")
        exit(1)

def process_pdf(file_path):
    with open(file_path, 'rb') as file:
        pdf_content = file.read()
    pdf_base64 = base64.b64encode(pdf_content).decode('utf-8')

    return {
        "_index": INDEX_NAME,
        "pipeline": PIPELINE_NAME,
        "data": pdf_base64,
        "file_name": os.path.basename(file_path)
    }

def bulk_with_retry(actions, max_retries=3, initial_backoff=5):
    for attempt in range(max_retries):
        try:
            success, failed = helpers.bulk(es, actions, stats_only=False, request_timeout=300)
            return success, failed
        except helpers.BulkIndexError as e:
            print(f"Bulk index error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise
        time.sleep(initial_backoff * (2 ** attempt))

def bulk_ingest_pdfs(folder_path, batch_size=10):
    pdf_files = glob.glob(os.path.join(folder_path, "*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in {folder_path}")
        return

    total_success = 0
    total_failed = 0

    for i in range(0, len(pdf_files), batch_size):
        batch = pdf_files[i:i+batch_size]
        
        print(f"Processing batch {i//batch_size + 1} of {len(pdf_files)//batch_size + 1}")
        
        actions = (process_pdf(pdf_file) for pdf_file in batch)
        
        try:
            success, failed = bulk_with_retry(actions)
            total_success += success
            total_failed += len(failed) if failed else 0
            print(f"Batch {i//batch_size + 1} complete: {success} succeeded, {len(failed) if failed else 0} failed")
        except Exception as e:
            print(f"Failed to process batch {i//batch_size + 1}: {e}")
            total_failed += len(batch)

    print(f"Ingestion complete. Total succeeded: {total_success}, Total failed: {total_failed}")

def main(args):
    if args.create_pipeline:
        create_pipeline()
    elif args.create_index:
        if not index_exists():
            create_index()
        else:
            print(f"Index '{INDEX_NAME}' already exists")
    elif args.delete_index:
        delete_index()
    elif args.delete_pipeline:
        delete_pipeline()
    elif args.folder:
        if not pipeline_exists():
            print(f"Pipeline '{PIPELINE_NAME}' does not exist. Please create it first using --create-pipeline")
            exit(1)
        if not index_exists():
            print(f"Index '{INDEX_NAME}' does not exist. Please create it first using --create-index")
            exit(1)
        bulk_ingest_pdfs(args.folder)
    else:
        print("Please specify either --create-pipeline, --create-index, --delete-index, --delete-pipeline, or --folder")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest PDFs into Elasticsearch")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--create-pipeline", action="store_true", help="Create the ingest pipeline")
    group.add_argument("--create-index", action="store_true", help="Create the Elasticsearch index")
    group.add_argument("--delete-index", action="store_true", help="Delete the Elasticsearch index")
    group.add_argument("--delete-pipeline", action="store_true", help="Delete the ingest pipeline")
    group.add_argument("--folder", help="Path to the folder containing PDF files")
    args = parser.parse_args()

    main(args)
