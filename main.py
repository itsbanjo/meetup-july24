import streamlit as st
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from openai import OpenAI
import os
from text_analysis import perform_text_analysis
from search import perform_search
from rag_search import perform_rag_search
from rag_search_notes import perform_rag_search_notes
from datetime import datetime
from operator import itemgetter

load_dotenv()

es = Elasticsearch(
    cloud_id=os.getenv("CLOUD_ID"),
    api_key=os.getenv("API_KEY")
)

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

ELASTIC_URL = os.getenv("ELASTIC_URL")
SENTIMENT_MODEL = os.getenv('SENTIMENT_MODEL')
NER_MODEL = os.getenv('NER_MODEL')
ZERO_SHOT_MODEL = os.getenv('ZERO_SHOT_MODEL')
ELSER_MODEL = os.getenv('ELSER_MODEL')

MODEL_MAP = {
    "Named Entity Recognition": NER_MODEL,
    "Sentiment Analysis": SENTIMENT_MODEL,
    "Zero Shot Recognition": ZERO_SHOT_MODEL
}

index_name_mapping = {
    "Blood Tests": "healthcare",
    "GP": "notes-healthcare",
    "Radiology": "healthcare-radiology",
    "Drug Trials": "healthcare-drugtrials",
    "Disease Studies": "healthcare-diseasestudies",
    "Patient Records": "healthcare-patientrecords",
    "Billing": "healthcare-billing"
}


def display_results(results, sort_field, sort_order):
    if results:
        reverse_sort = sort_order == "Descending"
        sorted_results = sorted(results, key=itemgetter(sort_field), reverse=reverse_sort)

        for i, result in enumerate(sorted_results, 1):
            with st.expander(f"Result {i}: {result.get('Patient name', 'N/A')}"):
                st.write(f"**NHI:** {result.get('NHI', 'N/A')}")
                st.write(f"**Date of Birth:** {result.get('Date of Birth', 'N/A')}")
                st.write(f"**GP:** {result.get('GP', 'N/A')}")
                st.write(f"**Condition:** {result.get('Condition', 'N/A')}")
                st.write(f"**Note Date:** {result.get('Note Date', 'N/A')}")
                st.write("**Clinical Notes:**")

                clinical_notes = result.get('Clinical Notes', 'N/A')
                if clinical_notes != 'N/A':
                    sentences = clinical_notes.split('. ')
                    formatted_notes = '. '.join(sentence.strip() for sentence in sentences if sentence)
                    st.write(formatted_notes)
                else:
                    st.write("No clinical notes available.")

                if 'Highlights' in result:
                    st.write("**Relevant Excerpts:**")
                    for highlight in result['Highlights']:
                        st.markdown(f"- ... {highlight} ...", unsafe_allow_html=True)
    else:
        st.write("No results found.")


def get_date_range(es, index_name):
    try:
        body = {
            "aggs": {
                "min_date": {"min": {"field": "note_date"}},
                "max_date": {"max": {"field": "note_date"}}
            },
            "size": 0
        }
        response = es.search(index=index_name, body=body)
        min_date = datetime.fromisoformat(response['aggregations']['min_date']['value_as_string'].split('T')[0])
        max_date = datetime.fromisoformat(response['aggregations']['max_date']['value_as_string'].split('T')[0])
        return min_date.date(), max_date.date()
    except Exception as e:
        print(f"Error getting date range: {str(e)}")
        return datetime.now().date(), datetime.now().date()

st.markdown("""
<style>
    em {
        background-color: lightblue;
        color: black;
        font-style: normal;
    }
</style>
""", unsafe_allow_html=True)

st.title("InsightMed")

st.sidebar.title("Menu")

usecase_options = {
    "Clinical Data": ["Blood Tests", "GP", "Radiology"],
    "Medical Research": ["Drug Trials", "Disease Studies"],
    "Healthcare Administration": ["Patient Records", "Billing"]
}

usecase_choice = st.sidebar.selectbox("Use-case options", list(usecase_options.keys()))
sub_category = st.sidebar.selectbox("Choose Sub-category", usecase_options[usecase_choice])

INDEX_NAME = index_name_mapping.get(sub_category, "default-index")

debug_mode = st.sidebar.checkbox("Enable Debug Mode")

col1, col2, col3 = st.columns(3)
for col in (col1, col2, col3):
    with col:
        st.image("artefacts/logo.jpg", width=150)

if 'current_tab' not in st.session_state:
    st.session_state.current_tab = "Text Analysis"

if 'date_range' not in st.session_state:
    st.session_state.date_range = None

tab1, tab2, tab3 = st.tabs(["Text Analysis", "Search", "RAG"])

with tab1:
    st.session_state.current_tab = "Text Analysis"
    
    analysis_type = st.selectbox("Choose analysis type", list(MODEL_MAP.keys()))
    
    text_input = st.text_area("Enter text for analysis")
    
    if st.button("Analyze"):
        selected_model = MODEL_MAP[analysis_type]
        
        result = perform_text_analysis(analysis_type, text_input, es, selected_model)
        st.write(result)

with tab2:
    st.session_state.current_tab = "Search"
    
    if st.session_state.date_range is None:
        st.session_state.date_range = get_date_range(es, INDEX_NAME)
    
    min_date, max_date = st.session_state.date_range
    
    search_query = st.text_input("Enter your search query")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", min_value=min_date, max_value=max_date, value=min_date)
    with col2:
        end_date = st.date_input("End Date", min_value=min_date, max_value=max_date, value=max_date)
    
    search_type = st.radio("Choose search type", ["Text Search", "ELSER Search", "Hybrid Search"])
    
    sort_field = st.selectbox("Sort by", ["Patient name", "Note Date", "NHI"])
    sort_order = st.radio("Sort order", ["Ascending", "Descending"])
    
    if st.button("Search"):
        results = perform_search(search_type, search_query, es, INDEX_NAME, ELSER_MODEL, start_date, end_date)
        
        st.subheader(f"{search_type} Results")
        display_results(results, sort_field, sort_order)
        
        if debug_mode:
            st.sidebar.subheader("Debug: Raw Search Results")
            st.sidebar.json(results)

with tab3:
    st.session_state.current_tab = "RAG"
    st.write("Welcome to the RAG (Retrieval-Augmented Generation) chatbot. Please use the chat input below to ask your questions.")
        
    if "messages" not in st.session_state:
        st.session_state.messages = []

    chat_container = st.container()
    input_container = st.container()

    with chat_container:
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    
    with input_container:
        if prompt := st.chat_input("What is your question?", key="chat_input"):
            st.session_state.messages.append({"role": "user", "content": prompt})
        
            with chat_container:
                st.chat_message("user").markdown(prompt)
            
            # Determine which function to call based on INDEX_NAME
            if INDEX_NAME == "notes-healthcare":
                esql_query, response = perform_rag_search_notes(prompt, es, openai_client, INDEX_NAME, ELSER_MODEL)
                print("HIT::: " + INDEX_NAME )
            elif INDEX_NAME == "healthcare":
                esql_query, response = perform_rag_search(prompt, es, openai_client, INDEX_NAME, ELSER_MODEL)
                print("HIT::: " + INDEX_NAME )
            else:
                esql_query, response = None, "Invalid INDEX_NAME"
                
            if esql_query:
                with chat_container:
                    with st.chat_message("assistant"):
                        st.write("Generated ESQL query:")
                        st.code(esql_query, language="sql")
        
            if isinstance(response, tuple):
                text_response, fig = response
                with chat_container:
                    with st.chat_message("assistant"):
                        st.write(text_response)
                        st.plotly_chart(fig)
                st.session_state.messages.append({"role": "assistant", "content": text_response})
            else:
                with chat_container:
                    st.chat_message("assistant").markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})


if st.sidebar.button("Reset Date Range"):
    st.session_state.date_range = None
    st.rerun()

if debug_mode:
    st.sidebar.title("Debug Information")
    st.sidebar.json(st.session_state.to_dict())

