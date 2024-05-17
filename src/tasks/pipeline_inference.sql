USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE DEMO_GITHUB_ACTIONS;

CREATE OR REPLACE TASK DEV.TASK_PROCESS
WAREHOUSE = COMPUTE_WH
SCHEDULE = '180 MINUTE'
AS
    CALL DEMO_GITHUB_ACTIONS.DEV.processing_step();

CREATE OR REPLACE TASK DEV.TASK_INFERENCE
WAREHOUSE = COMPUTE_WH
AFTER DEV.TASK_PROCESS
AS
    CALL DEMO_GITHUB_ACTIONS.DEV.inference_step();
;

ALTER TASK DEV.TASK_PROCESS RESUME;
ALTER TASK DEV.TASK_INFERENCE RESUME;



