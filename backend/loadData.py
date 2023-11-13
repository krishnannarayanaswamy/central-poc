import time
import os
import cassio
import json
import pandas as pd
import openai

from test_pattern import OpenAITestPattern
from cassio.config import check_resolve_session


cassio.init(
    token=os.environ['ASTRA_DB_APPLICATION_TOKEN'],
    database_id=os.environ['ASTRA_DB_DATABASE_ID'],
    keyspace=os.environ.get('ASTRA_DB_KEYSPACE'),
)

def translate_lang(query):
    message_objects = []
    message_objects.append({"role":"user",
                        "content": "You are a home improvement store. The product inventory contains some mix of thai and english content. Translate the complete text to English :'" +  query + "'"})
    completion = openai.ChatCompletion.create(
    model="gpt-4-1106-preview", 
    messages=message_objects
    )
    text_in_en = completion.choices[0].message.content
   
    return text_in_en

with open('data/export_product_5000.json') as f:
    data = json.load(f)

result = pd.json_normalize(data)

llmtexts = []

start = 250
batch_size = 50
for i in range(start, start+batch_size, batch_size):
    print(f"Processing {i} to {i+batch_size} llm texts")
    batch = result[i:i+batch_size]
    for id, row in batch.iterrows():
        specs = pd.json_normalize(row['spec'])
        spectext = ""
        for id, spec in specs.iterrows():
            spectext = spectext + f"{spec['name']}: {spec['value']} "
            #print(spectext)
        rawtext = f"Product name: {row['product_name_en']} Brand: {row['brand']} Product category: {row['product_categories']} Short Description: {row['short_description_en']} Available: {row['availability']} Price: {row['sale_price']} Description: {row['long_description_en']} Specifications: {spectext}"
        print(row['product_id'])
        translated_text = translate_lang(rawtext)
        #clean_text = translated_text.replace("\n", "")
        llmtexts.append(translated_text)
        print(llmtexts)
    batch['llmtext'] = llmtexts
    batch.to_csv(f"data/llm_product_{start}.csv", encoding='utf-8', index=False)
    test_pattern = OpenAITestPattern(session=check_resolve_session(None), model_name='text-embedding-ada-002',
                                 api_key=os.environ['OPENAI_API_KEY'],
                                 keyspace='ecommerce',
                                 table_name='central_openai_en')
    # To make Product ID as document ID, use `adds_texts` instead of `from_documents`
    vstore = test_pattern.vectore_store()
    print(f"Adding {i} to {i+batch_size} vector store")
    if start == 0:
        vstore.clear()
    vstore.add_texts(texts=llmtexts,
                     metadatas=batch.to_dict(orient='records'),
                     ids=batch['product_id'].to_list())


