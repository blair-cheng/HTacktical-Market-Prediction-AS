CREATE OR REPLACE MODEL `YOUR_PROJECT_ID.historical_mirror_mvp.embedding_model`
REMOTE WITH CONNECTION `YOUR_PROJECT_ID.US.bq_llm_connection`
OPTIONS (endpoint = 'text-embedding-004');

CREATE OR REPLACE MODEL `YOUR_PROJECT_ID.historical_mirror_mvp.gemini_model`
  REMOTE WITH CONNECTION `YOUR_PROJECT_ID.US.bq_llm_connection`
  OPTIONS (endpoint = 'gemini-2.5-pro');
 