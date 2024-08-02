# main.py
import streamlit as st
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from openai import OpenAI
import os
from text_analysis import perform_text_analysis
from search import perform_search
from rag_search import perform_rag_search

# Load environment variables
load_dotenv()

# Initialize Elasticsearch client
es = Elasticsearch(
    cloud_id=os.getenv("CLOUD_ID"),
    api_key=os.getenv("API_KEY")
)

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

# Streamlit UI
st.title("Healthcare ChatBot")

# Sidebar
st.sidebar.title("Menu")

# Use case options (you can customize these)
usecase_options = {
    "Clinical Data": ["Blood Tests", "Radiology", "Patient History"],
    "Medical Research": ["Drug Trials", "Disease Studies"],
    "Healthcare Administration": ["Patient Records", "Billing"]
}

usecase_choice = st.sidebar.selectbox(
    "Use-case options",
    list(usecase_options.keys())
)

sub_category = st.sidebar.selectbox(
    "Choose Sub-category",
    usecase_options[usecase_choice]
)

# File uploader (optional)
uploaded_file = st.sidebar.file_uploader("Choose a file", type=["csv", "txt", "pdf"])
if uploaded_file is not None:
    st.sidebar.write(f"Uploaded file: {uploaded_file.name}")

# Debug mode toggle
debug_mode = st.sidebar.checkbox("Enable Debug Mode")

# Display logos
col1, col2, col3 = st.columns(3)
with col1:
    st.image("artefacts/elastic.png", width=150)
with col2:
    st.image("artefacts/elastic.png", width=150)
with col3:
    st.image("artefacts/elastic.png", width=150)

# Main interface with tabs
tab1, tab2, tab3 = st.tabs(["Text Analysis", "Search", "RAG"])

with tab1:
    analysis_type = st.selectbox("Choose analysis type", ["Named Entity Recognition", "Sentiment Analysis", "Zero Shot Recognition"])
    text_input = st.text_area("Enter text for analysis")
    if st.button("Analyze"):
        result = perform_text_analysis(analysis_type, text_input, client)
        st.write(result)

with tab2:
    search_type = st.radio("Choose search type", ["Text Search", "RRF Search", "ELSER Search", "Hybrid Search"])
    search_query = st.text_input("Enter your search query")
    if st.button("Search"):
        results = perform_search(search_type, search_query, es)
        st.write(results)

with tab3:
    rag_query = st.text_input("Enter your RAG query")
    if st.button("Generate"):
        response = perform_rag_search(rag_query, es, client)
        st.write(response)

# Chat interface (common to all tabs)
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("What is your question?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").markdown(prompt)
    
    # Here you would process the prompt and generate a response
    # For now, let's just echo the prompt
    response = f"You said: {prompt}"
    
    st.session_state.messages.append({"role": "assistant", "content": response})
    st.chat_message("assistant").markdown(response)

# Debug information
if debug_mode:
    st.sidebar.title("Debug Information")
    # Add your debug information here
