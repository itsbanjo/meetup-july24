from datetime import datetime

def perform_search(search_type, query, es, index_name, model_id, start_date, end_date):
    if search_type == "Text Search":
        return text_search(query, es, index_name, start_date, end_date)
    elif search_type == "RRF Search":
        return rrf_search(query, es, index_name, start_date, end_date)
    elif search_type == "ELSER Search":
        return elser_search(query, es, index_name, model_id, start_date, end_date)
    elif search_type == "Hybrid Search":
        return hybrid_search(query, es, index_name, model_id, start_date, end_date)
    else:
        return "Invalid search type"

def text_search(query, es, index_name, start_date, end_date):
    try:
        should_conditions = [
            {"match_phrase": {"clinical_note": {"query": query, "slop": 3}}},
            {"match": {"clinical_note": {"query": query, "fuzziness": "AUTO"}}},
            {"multi_match": {
                "query": query,
                "fields": ["clinical_note^3", "condition^2", "patient_name", "gp"],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }}
        ]
        
        must_conditions = [
            {"range": {"note_date": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}}
        ]
        
        body = {
            "query": {
                "bool": {
                    "should": should_conditions,
                    "must": must_conditions,
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "clinical_note": {},
                    "condition": {},
                    "patient_name": {},
                    "gp": {}
                }
            }
        }
        response = es.search(index=index_name, body=body, size=20)  # Increase size if needed
        return process_results(response, include_highlights=True)
    except Exception as e:
        return f"Error performing Text Search: {str(e)}"

def elser_search(query, es, index_name, model_id, start_date, end_date):
    try:
        must_conditions = [
            {
                "text_expansion": {
                    "text_embedding": {
                        "model_id": model_id,
                        "model_text": query
                    }
                }
            },
            {"range": {"note_date": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}}
        ]
        
        body = {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "highlight": {
                "fields": {
                    "clinical_note": {},
                    "condition": {},
                    "patient_name": {},
                    "gp": {}
                }
            }
        }
        response = es.search(index=index_name, body=body)
        return process_results(response, include_highlights=True)
    except Exception as e:
        return f"Error performing ELSER Search: {str(e)}"

def hybrid_search(query, es, index_name, model_id, start_date, end_date):
    try:
        must_conditions = [
            {
                "bool": {
                    "should": [
                        {"match": {"clinical_note": query}},
                        {
                            "text_expansion": {
                                "text_embedding": {
                                    "model_id": model_id,
                                    "model_text": query
                                }
                            }
                        }
                    ]
                }
            },
            {"range": {"note_date": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}}
        ]
        
        body = {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "highlight": {
                "fields": {
                    "clinical_note": {},
                    "condition": {},
                    "patient_name": {},
                    "gp": {}
                }
            }
        }
        response = es.search(index=index_name, body=body)
        return process_results(response, include_highlights=True)
    except Exception as e:
        return f"Error performing Hybrid Search: {str(e)}"

def process_results(response, include_highlights=False):
    results = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        result = {
            "Patient name": source.get('patient_name', 'N/A'),
            "NHI": source.get('nhi', 'N/A'),
            "Date of Birth": source.get('dob', 'N/A'),
            "GP": source.get('gp', 'N/A'),
            "Condition": source.get('condition', 'N/A'),
            "Note Date": source.get('note_date', 'N/A'),
            "Clinical Notes": source.get('clinical_note', 'N/A')
        }
        if include_highlights and 'highlight' in hit:
            result['Highlights'] = []
            for field, highlights in hit['highlight'].items():
                result['Highlights'].extend(highlights)
        results.append(result)
    return results

def rrf_search(query, es, index_name, start_date, end_date):
    try:
        # Implement your RRF search logic here
        # For now, we'll use a basic text search as a placeholder
        body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"clinical_note": query}},
                        {"range": {"note_date": {"gte": start_date.isoformat(), "lte": end_date.isoformat()}}}
                    ]
                }
            },
            "highlight": {
                "fields": {
                    "clinical_note": {},
                    "condition": {},
                    "patient_name": {},
                    "gp": {}
                }
            }
        }
        response = es.search(index=index_name, body=body, size=20)
        return process_results(response, include_highlights=True)
    except Exception as e:
        print(f"Error performing RRF Search: {str(e)}")
        return []  # Return an empty list in case of error
