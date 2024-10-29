use database <your_db>;
use schema <your_schema>;
use warehouse <your_wh>;

--Create a stream to listen for new documents
create or replace stream docs_stream on stage docs;

--Create a task to process the new documents when detected in the stream
create or replace task parse_and_insert_pdf_task 
    warehouse = COMPUTE_WH
    schedule = '1 minute'
    when system$stream_has_data('docs_stream')
    as
  
    insert into docs_chunks_table (relative_path, size, file_url,
                            scoped_file_url, chunk)
    select relative_path, 
            size,
            file_url, 
            build_scoped_file_url(@docs, relative_path) as scoped_file_url,
            func.chunk as chunk
    from 
        docs_stream,
        TABLE(text_chunker (TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@docs, relative_path, {'mode': 'LAYOUT'})))) as func;

--Resume the task
alter task parse_and_insert_pdf_task resume;

--Upload a document; wait a minute; verify that the document has been processed
select * from docs_stream;

--Suspend the task once finished
alter task parse_and_insert_pdf_task suspend;
