# scholar_citation_summarization
summarize your citation information from google scholar page. return paper, author, institution, regions citing your work
# usage
put your google sholar id in 
    scholar_id = "XXXXxxxxxxx" # your google scholar id
Then
    python scholar_citations.py
Once obtained citations, change csv file name in scrape_affliations.py
    input_file = 'scholar_citations_{scholar_id}_{timestamp}.csv' # change this
    output_file = 'scholar_citations_{scholar_id}_{timestamp}_affliations.csv' # change this
Then
    python scrape_affliations.py
