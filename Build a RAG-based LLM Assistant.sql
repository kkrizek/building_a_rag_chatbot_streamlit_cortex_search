--ORGANIZE DOCUMENTS AND CREATE PRE-PROCESSING FUNCTION
/*
Create a table function that will split text into chunks

We will be using the Langchain Python library to accomplish the necessary document split tasks. Because as part of Snowpark Python these are available inside the 
integrated Anaconda repository, there are no manual installs or Python environment and dependency management required.
*/

create or replace function text_chunker(pdf_text string)
returns table (chunk varchar)
language python
runtime_version = '3.9'
handler = 'text_chunker'
packages = ('snowflake-snowpark-python', 'langchain')
as
$$
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def process(self, pdf_text: str):
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size = 1512, #Adjust this as you see fit
            chunk_overlap  = 256, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len
        )
    
        chunks = text_splitter.split_text(pdf_text)
        df = pd.DataFrame(chunks, columns=['chunks'])
        
        yield from df.itertuples(index=False, name=None)
$$;

-- Create a Stage with Directory Table where you will host your documents
create or replace stage docs ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

-- The documents are stored in an integrated GIT repository.  There should be one GIT repository in your database
show git repositories;

-- Copy the Documents
COPY FILES INTO @docs/
    FROM @<NAME of the git repository>/branches/main/docs/;

ls @docs;

--PRE-PROCESS AND LABEL DOCUMENTS
--Create the table where we are going to store the chunks for each PDF.
create or replace TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    FILE_URL VARCHAR(16777216), -- URL for the PDF
    SCOPED_FILE_URL VARCHAR(16777216), -- Scoped url (you can choose which one to keep depending on your use case)
    CHUNK VARCHAR(16777216), -- Piece of text
    CATEGORY VARCHAR(16777216) -- Will hold the document category to enable filtering
);

/*
Function SNOWFLAKE.CORTEX.PARSE_DOCUMENT will be used to read the PDF documents directly from the staging area. The text will be passed to the function 
previously created to split the text into chunks. There is no need to create embeddings as that will be managed automatically by Cortex Search service later.
*/
insert into docs_chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk)
    select relative_path, 
            size,
            file_url, 
            build_scoped_file_url(@docs, relative_path) as scoped_file_url,
            func.chunk as chunk
    from 
        directory(@docs),
        TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@docs, 
                              relative_path, {'mode': 'LAYOUT'})))) as func;


/*
We are going to use the power of Large Language Models to easily classify the documents we are ingesting in our RAG application. We are just going to use the file name 
but you could also use some of the content of the doc itself.

First we will create a temporary table with each unique file name and we will be passing that file name to one LLM using Cortex Complete function with a prompt to classify 
what that use guide refres too. The prompt will be as simple as this but you can try to customize it depending on your use case and documents. Classification is not mandatory 
for Cortex Search but we want to use it here to also demo hybrid search.
*/
CREATE
OR REPLACE TEMPORARY TABLE docs_categories AS WITH unique_documents AS (
  SELECT
    DISTINCT relative_path
  FROM
    docs_chunks_table
),
docs_category_cte AS (
  SELECT
    relative_path,
    TRIM(snowflake.cortex.COMPLETE (
      'llama3-70b',
      'Given the name of the file between <file> and </file> determine if it is related to bikes or snow. Use only one word <file> ' || relative_path || '</file>'
    ), '\n') AS category
  FROM
    unique_documents
)
SELECT
  *
FROM
  docs_category_cte;

--Check the table to identify how many categories have been created
select category from docs_categories group by category;

/*
Now we can just update the table with the chunks of text that will be used by Cortex Search service to include the category for each document:
*/
update docs_chunks_table 
  SET category = docs_categories.category
  from docs_categories
  where  docs_chunks_table.relative_path = docs_categories.relative_path;

--CREATE CORTEX SEARCH SERVICE
create or replace CORTEX SEARCH SERVICE CC_SEARCH_SERVICE_CS
ON chunk
ATTRIBUTES category
warehouse = <YOUR_WH>
TARGET_LAG = '1 minute'
as (
    select chunk,
        relative_path,
        file_url,
        category
    from docs_chunks_table
);

