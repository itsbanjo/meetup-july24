

Quickstart:

1. clone this demo
2. make sure you have python running
3. preferabbly run using venv
4. update the .env


streamlit run main.py


To perform the RAG demo with actionable insights:

1. Generate a simulated blood report  
  python 1-generate-blood-report.py --patients 15 --samples 5 --start-year 2020 --end-year 2022 --percentage-min 5 --percentage-max 10 --to-pdf --output-dir ./reports


2. Create a pipeline and index

  python 2-upload-blood-report.py --create-pipeline 
  python 2-upload-blood-report.py --create-index


3. Upload the PDF reports to your ES instance
  python 2-upload-blood-report.py --folder reports/



To do the Keyword, ELSER, and Hybrid Demo

1. Create index
   python 3-generate-and-upload-clinical-report.py --create-index
2. Create pipeline
   python 3-generate-and-upload-clinical-report.py --create-pipeline
3. Generate clinical data demo
   python 3-generate-and-upload-clinical-report.py --input-csv list_conditions.txt
