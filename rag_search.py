# rag_search.py
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go



def perform_rag_search(query, es, openai_client, index_name, model_id):
    print("DEBUG:: " + query)
    query_type, visualization_type = classify_query(query, openai_client)
    print("DEBUG:: query_type: " + str(query_type) + ' ' + "visaulization_type: " + str(visualization_type))

    
    if query_type == "1":  # Graph or visual request
        esql_query, response = handle_visualization_request(query, es, openai_client, index_name, visualization_type)
        return esql_query, response
    else:
        # Proceed with regular RAG search
        context = retrieve_documents(query, es, index_name, model_id)
        print("DEBUG: context: " + context )
        return None, generate_response(context, query, openai_client)


def classify_query(prompt, openai_client):
    classification_prompt = f"""Classify the following query into one of these categories:
    1. Asking for generating graph, visuals, or table
    2. Other query

    If category 1, also specify the visualization type (line, bar, area, scatter, or table).

    Query: {prompt}

    Respond with the category number followed by the visualization type if applicable, e.g., "1 line", "1 table", or "2"
    """
    
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that classifies medical queries."},
            {"role": "user", "content": classification_prompt}
        ],
        max_tokens=10,
        n=1,
        temperature=0.3,
    )
    
    result = response.choices[0].message.content.strip().split()
    query_type = result[0]
    visualization_type = result[1] if len(result) > 1 else "line"  # Default to line graph
    return query_type, visualization_type

def handle_visualization_request(query, es, openai_client, index_name, visualization_type):
    patient_info, blood_params = extract_patient_info(query, openai_client)
    
    if patient_info["name"] == "None" and patient_info["nhi"] == "None":
        return None, "I'm sorry, but I couldn't identify a patient name or NHI in your request. Could you please rephrase your question and include the patient's name or NHI?"
    
    esql_query = generate_esql(patient_info, blood_params, index_name, openai_client)
    print("DEBUG: esql_query: " + esql_query)
    result = execute_esql(esql_query, es)
    print("DEBUG: result: " + str(result))
    
    if result and result.get('rows'):
        df = pd.DataFrame(result['rows'], columns=[col['name'] for col in result['columns']])
        
        if visualization_type == "table":
            fig = create_table(df)
            response = "I've generated a table with the requested blood test results."
        else:
            fig = plot_blood_count_graph(df, visualization_type)
            response = f"I've generated a historical {visualization_type} graph of blood count for the patient. The visualization shows the trends for {', '.join(blood_params)} over time."
        
        return esql_query, (response, fig)
    else:
        patient_identifier = patient_info["name"] if patient_info["name"] != "None" else patient_info["nhi"]
        return esql_query, f"I'm sorry, but I couldn't find any blood count data for {patient_identifier}."



def create_table(df):
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=list(df.columns),
            fill_color='white',
            align='left',
            font=dict(color='black', size=12),
            line=dict(color='black', width=1)
        ),
        cells=dict(
            values=[df[col] for col in df.columns],
            fill_color='white',
            align='left',
            font=dict(color='black', size=11),
            line=dict(color='black', width=1)
        )
    )])
    fig.update_layout(
        title="Blood Test Results",
        title_font=dict(size=16, color='black'),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    return fig

def plot_blood_count_graph(df, graph_type):
    df['test_date'] = pd.to_datetime(df['test_date'])
    
    plot_functions = {
        "line": px.line,
        "bar": px.bar,
        "area": px.area,
        "scatter": px.scatter
    }
    
    plot_function = plot_functions.get(graph_type, px.line)  # Default to line if unknown type
    
    fig = plot_function(df, x='test_date', y=df.columns[3:], title=f'Historical Blood Count ({graph_type.capitalize()} Graph)')
    
    # Customize layout for better readability
    fig.update_layout(
        legend_title_text='Blood Parameters',
        xaxis_title='Test Date',
        yaxis_title='Value',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig

def extract_patient_info(prompt, openai_client):
    print("DEBUG:: " + prompt)
    extraction_prompt = f"""Extract the following information from the query:
    1. Patient name (if available)
    2. NHI (if available)
    3. Requested blood parameters (from the list: haemoglobin, wbc, rbc, platelets, neutrophils, lymphocytes, monocytes, eosinophils, basophils)

    Query: {prompt}

    Respond in the format:
    Patient Name: <extracted name or None>
    NHI: <extracted NHI or None>
    Blood Parameters: <comma-separated list of requested parameters or 'All' if not specified>
    """
    
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that extracts patient information and requested blood parameters from queries."},
            {"role": "user", "content": extraction_prompt}
        ],
        max_tokens=100,
        n=1,
        temperature=0.3,
    )
    
    result = response.choices[0].message.content.strip()
    
    lines = result.split('\n')
    patient_name = lines[0].split(': ')[1] if len(lines) > 0 else 'None'
    nhi = lines[1].split(': ')[1] if len(lines) > 1 else 'None'
    blood_params = lines[2].split(': ')[1] if len(lines) > 2 else 'All'
    
    if blood_params == 'All':
        blood_params = ['haemoglobin', 'wbc', 'rbc', 'platelets', 'neutrophils', 'lymphocytes', 'monocytes', 'eosinophils', 'basophils']
    else:
        blood_params = [param.strip() for param in blood_params.split(',')]
    
    return {"name": patient_name, "nhi": nhi}, blood_params


def generate_esql(patient_info, blood_params, index_name, openai_client):
    patient_clause = f"MATCH(patient_name, '{patient_info['name']}')" if patient_info["name"] != "None" else f"nhi = '{patient_info['nhi']}'"
    select_params = ', '.join(blood_params)
    prompt = f"""Generate an E|SQL query to fetch historical blood count data for a patient with the following information:
    Patient Info: {patient_info}
    Blood Parameters: {select_params}
    Index Name: {index_name}
    The query should:
    1. Use the specified index name in the FROM clause.
    2. Select patient_name, nhi, test_date, and all the specified blood parameters.
    3. Include a WHERE clause to filter using: {patient_clause}
    4. Order the results by test_date.
    5. Use lowercase for all column names except NHI.
    6. Do not use 'AS' aliases for column names.
    IMPORTANT: Only return the E|SQL query, nothing else. Do not include any explanations, additional text, or semicolons at the end.
    """
    
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are an expert in generating E|SQL queries for Elasticsearch. Only return the query, nothing else. Do not include semicolons. Always use the provided index name in the FROM clause."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=200,
        n=1,
        temperature=0.7,
    )
    
    esql_query = response.choices[0].message.content.strip()
    
    if not esql_query.upper().startswith("SELECT"):
        esql_query = "SELECT " + esql_query
    esql_query = esql_query.rstrip(';')
    
    # Post-processing: ensure all column names are lowercase except NHI
    parts = esql_query.split()
    for i, part in enumerate(parts):
        if part.upper() != 'NHI' and part.upper() != 'FROM':
            parts[i] = part.lower()
        elif part.upper() == 'FROM':
            break  # Stop processing after reaching the FROM clause
    
    esql_query = ' '.join(parts)
    
    return esql_query

def execute_esql(esql_query, es):
    try:
        response = es.sql.query(body={"query": esql_query})
        return response
    except Exception as e:
        print(f"Error executing E|SQL query: {str(e)}")
        print(f"Query: {esql_query}")
        
        # If the error is due to an unknown column, try to get the available columns
        if "Unknown column" in str(e):
            try:
                # Query to get the first row and its keys
                sample_query = f"SELECT * FROM {index_name} LIMIT 1"
                sample_response = es.sql.query(body={"query": sample_query})
                if sample_response and 'rows' in sample_response and len(sample_response['rows']) > 0:
                    available_columns = sample_response['columns']
                    print(f"Available columns: {[col['name'] for col in available_columns]}")
            except Exception as inner_e:
                print(f"Error while trying to fetch available columns: {str(inner_e)}")
        
        return None

def retrieve_documents(query, es, index_name, model_id):
    print("Query: " + query + " index_name " + index_name + " model_id: " + model_id)
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
                    },
                    {
                        "multi_match": {
                            "query": query,
                            "fields": [
                                "patient_name^2",
                                "nhi",
                                "lab",
                                "address",
                                "haemoglobin",
                                "wbc",
                                "rbc",
                                "platelets",
                                "neutrophils",
                                "lymphocytes",
                                "monocytes",
                                "eosinophils",
                                "basophils",
                                "test_date",
                                "*"
                            ],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    }
                ]
            }
        }
    }
    response = es.search(index=index_name, body=body)
    print("DEBUG:: " + str(response))
    
    context = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        hit_context = ". ".join(f"{key}: {value}" for key, value in source.items() if value)
        context.append(hit_context)
    
    return "\n\n".join(context)

def generate_response(context, query, openai_client):
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that answers questions based on the given context."},
            {"role": "user", "content": f"Context: {context}\n\nQuestion: {query}"}
        ],
        max_tokens=150,
        n=1,
        temperature=0.7,
    )
    return response.choices[0].message.content
