# rag_search_notes.py

def perform_rag_search_notes(query, es, openai_client, index_name, model_id):
    response = retrieve_documents(query, es, openai_client, index_name, model_id)
    context = extract_clinical_notes(response)
    return None, generate_response(context, query, openai_client)

def extract_clinical_notes(response):
    clinical_notes = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        clinical_note = source.get('clinical_note', 'N/A')
        condition = source.get('condition', 'N/A')
        
        # Create an anonymized summary for each note
        anonymized_note = f"Condition: {condition}. Clinical note: {clinical_note}"
        clinical_notes.append(anonymized_note)
    
    # Join all clinical notes into a single string, separating each note clearly
    combined_notes = "\n\n".join(clinical_notes)
    
    print("DEBUG:::::::::" + combined_notes)
    return combined_notes

def retrieve_documents(query, es, openai_client, index_name, model_id):
    body = {
        "size": 5,
        "query": {
            "bool": {
                "should": [
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
        }
    }
    response = es.search(index=index_name, body=body)
    return response

def generate_response(context, query, openai_client):
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": """You are an AI assistant specializing in medical information. Your task is to provide concise, accurate summaries or answers based on the given clinical notes. Each note may represent a different case. Focus on the medical aspects and avoid mentioning any specific patient details.

            Guidelines:
            1. Summarize key medical information from the notes relevant to the query.
            2. If multiple conditions are mentioned, address them separately if relevant.
            3. Provide general medical insights based on the information given.
            4. Do not invent or assume information not present in the notes.
            5. If the query cannot be answered based on the given information, state this clearly.
            6. Maintain a professional and empathetic tone."""},
            
            {"role": "user", "content": f"""Clinical Notes:
            {context}

            Query: {query}

            Please provide a concise and informative response based on the relevant information in these clinical notes. Focus on medical aspects and avoid referencing specific patients."""}
        ],
        max_tokens=250,  # Increased for more comprehensive responses
        n=1,
        temperature=0.5,  # Reduced for more consistent outputs
    )
    return response.choices[0].message.content
