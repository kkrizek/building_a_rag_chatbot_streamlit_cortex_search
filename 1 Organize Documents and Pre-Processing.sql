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

