USE ROLE $ROLE$;
USE WAREHOUSE $WAREHOUSE$;
USE DATABASE $DB_NAME$;
USE SCHEMA $SCHEMA$;

CREATE OR REPLACE TABLE  $DB_NAME$.$SCHEMA$.BANANA_QUALITY_RAW(
    SIZE DOUBLE NULL,
    WEIGHT DOUBLE NULL,
    SWEETNESS DOUBLE NULL,
    SOFTNESS DOUBLE NULL,
    HARVESTTIME DOUBLE NULL,
    RIPENESS DOUBLE NULL,
    ACIDITY DOUBLE NULL,
    QUALITY VARCHAR(4) NULL
);

CREATE OR REPLACE FILE FORMAT $DB_NAME$.$SCHEMA$.MY_CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true;

COPY INTO $DB_NAME$.$SCHEMA$.BANANA_QUALITY_RAW
FROM @$DB_NAME$.$SCHEMA$.RAW_DATA/banana_quality_raw.csv
FILE_FORMAT = (FORMAT_NAME = MY_CSV_FORMAT);
