CREATE TABLE pipelines (
pipeline_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
pipeline_code VARCHAR2(50) UNIQUE NOT NULL,
is_active CHAR(1)
	DEFAULT 'Y'
	CONSTRAINT chk_is_active
	CHECK (is_active IN ('Y', 'N')),
pipeline_name VARCHAR2(100) NOT NULL,
pipeline_descr VARCHAR2(300),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO pipelines (PIPELINE_CODE, IS_ACTIVE, PIPELINE_NAME, PIPELINE_DESCR)
VALUES ('AI_SUPPORT', 'Y', 'Support Ticket Pipeline', 'End-to-end pipeline that generates, validates, enriches and loads synthetic support ticket data.');


CREATE TABLE pipeline_stages (
stage_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
pipeline_id NUMBER NOT NULL
	CONSTRAINT fk_pipeline_stages_pipelines
	REFERENCES pipelines(pipeline_id),
stage_order NUMBER NOT NULL,
stage_name VARCHAR2(100) NOT NULL,
stage_descr VARCHAR2(300),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
CONSTRAINT uk_pipeline_stage
	UNIQUE (pipeline_id, stage_order),
CONSTRAINT uk_pipeline_stage_name
	UNIQUE (pipeline_id, stage_name)
);
INSERT INTO pipeline_stages (PIPELINE_ID, STAGE_ORDER, STAGE_NAME, STAGE_DESCR)
VALUES (1, 1, 'Generate Raw Data', 'Creates synthetic support ticket records and uploads them to Bronze storage.'),
(1, 2, 'Validate Bronze Data', 'Validates required fields, timestamps, regions, and duplicate records.'),
(1, 3, 'AI Enrichment', 'Classifies ticket category and sentiment score using Gemini with TextBlob fallback.'),
(1, 4, 'Load to Lakehouse', 'Maps dimension keys and loads the curated dataset into Oracle ADW.');


CREATE TABLE pipeline_runs (
run_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
execution_id VARCHAR2(20) NOT NULL,
stage_id NUMBER NOT NULL
	CONSTRAINT fk_pipeline_runs_stages
	REFERENCES pipeline_stages(stage_id),
run_status VARCHAR2(20)
	CONSTRAINT chk_run_status
	CHECK (run_status IN ('STARTED', 'SUCCESS', 'FAILED', 'SKIPPED')),
run_start_time TIMESTAMP,
run_end_time TIMESTAMP,
duration_seconds NUMBER GENERATED ALWAYS AS (
	ROUND((CAST(run_end_time AS DATE) - CAST(run_start_time AS DATE)) * 86400, 2)
	)VIRTUAL,
metrics JSON,
error_message VARCHAR2(1000)
);


CREATE TABLE configuration (
config_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
config_key VARCHAR2(100) UNIQUE NOT NULL,
config_value VARCHAR2(200) NOT NULL,
config_descr VARCHAR2(300),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO configuration (CONFIG_KEY, CONFIG_VALUE)
VALUES ('MODEL_NAME', 'gemini-2.5-flash'),
('MAX_RETRIES', '3'),
('API_DELAY_SECONDS', '3');