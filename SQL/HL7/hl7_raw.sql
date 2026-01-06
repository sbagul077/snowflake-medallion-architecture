
/*==============================================================================
  FINAL ONE-PASTE SCRIPT (NO TASKS, NO STREAMS, NO DYNAMISM)
  - Preserves all table names from your prior scripts.
  - Adds patient-id enrichment to EVERY segment using the best PID-3 identifier.
  - Provides file format, stage, and manual COPY INTO (no Snowpipe/streams).
  - You can re-run the INSERT blocks any time to refresh from HL7_BRONZE_BASE.
==============================================================================*/

-- 0) Context + Schema + Warehouse (safe if already present)
CREATE DATABASE IF NOT EXISTS HL7_FINAL_ASSIGNMENT;
USE DATABASE HL7_FINAL_ASSIGNMENT;

CREATE SCHEMA IF NOT EXISTS HL7;
USE SCHEMA HL7;


-- 1) File Format + Stage (as requested; uses your integration name)
CREATE OR REPLACE FILE FORMAT HL7_BRONZE_FF
  TYPE = 'CSV'
  FIELD_DELIMITER = '\t'
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  NULL_IF = ('', 'NULL')
  FIELD_OPTIONALLY_ENCLOSED_BY = NONE;

CREATE OR REPLACE STAGE HL7_STAGE
  URL = 's3://da-batch2-group1-capstone/'
  STORAGE_INTEGRATION = S3_STORAGE_INTEGRATION
  FILE_FORMAT = HL7_BRONZE_FF;

-- Optional quick checks
LIST @HL7_STAGE;
SHOW STAGES IN SCHEMA FINAL_ASSIGNMENT.HL7;

-- 2) Helper table (kept as-is)
CREATE OR REPLACE TABLE HL7_RAW_LINES ( LINE STRING );

-- 3) BRONZE RAW (no auto-ingest; use manual COPY INTO from the external stage)
CREATE OR REPLACE TABLE HL7_BRONZE_RAW (
  FILENAME        STRING,
  FILE_ROW_NUMBER NUMBER,
  SEGMENT_TYPE    STRING,
  LINE            STRING,
  LOAD_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- MANUAL load from stage to HL7_BRONZE_RAW (run whenever you want to ingest):
COPY INTO HL7_BRONZE_RAW (FILENAME, FILE_ROW_NUMBER, SEGMENT_TYPE, LINE, LOAD_TS)
FROM (
  SELECT
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    SPLIT_PART($1, '|', 1),   -- MSH/PID/PV1/...
    $1,
    CURRENT_TIMESTAMP()
  FROM @HL7_STAGE (FILE_FORMAT => HL7_BRONZE_FF)
)
FILE_FORMAT = (FORMAT_NAME = HL7_BRONZE_FF)
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.(hl7|txt)$';

-- 4) BRONZE BASE VIEW + TABLE (materialize from RAW)
CREATE OR REPLACE VIEW HL7_BRONZE_BASE_V AS
SELECT
  FILENAME,
  FILE_ROW_NUMBER,
  SEGMENT_TYPE,
  LINE,
  LAST_VALUE(
    CASE WHEN SEGMENT_TYPE = 'MSH' THEN SPLIT_PART(LINE, '|', 10) END
  ) IGNORE NULLS
  OVER (
    PARTITION BY FILENAME
    ORDER BY FILE_ROW_NUMBER
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS MESSAGE_ID,
  CURRENT_TIMESTAMP() AS PROCESS_TS
FROM HL7_BRONZE_RAW;

CREATE OR REPLACE TABLE HL7_BRONZE_BASE (
  FILENAME        STRING,
  FILE_ROW_NUMBER NUMBER,
  SEGMENT_TYPE    STRING,
  LINE            STRING,
  MESSAGE_ID      STRING,
  PROCESS_TS      TIMESTAMP_NTZ
);

-- Refresh BRONZE BASE from the view (re-run to refresh after new loads)
TRUNCATE TABLE HL7_BRONZE_BASE;
INSERT INTO HL7_BRONZE_BASE
SELECT FILENAME, FILE_ROW_NUMBER, SEGMENT_TYPE, LINE, MESSAGE_ID, PROCESS_TS
FROM HL7_BRONZE_BASE_V;

-- 5) Common patient-id helpers (derive the “best” id per MESSAGE_ID from PID-3)
--    You can leave these as CTEs inside INSERTs below; here we make temporary views for clarity.

CREATE OR REPLACE TEMP VIEW PID_ID_LIST_V AS
SELECT MESSAGE_ID, SPLIT_PART(LINE,'|',4) AS patient_id_list
FROM HL7_BRONZE_BASE
WHERE SEGMENT_TYPE='PID';

CREATE OR REPLACE TEMP VIEW PID_ONE_V AS
SELECT *
FROM PID_ID_LIST_V
QUALIFY ROW_NUMBER() OVER (PARTITION BY MESSAGE_ID ORDER BY MESSAGE_ID) = 1;

CREATE OR REPLACE TEMP VIEW PID_IDS_V AS
SELECT p.MESSAGE_ID, f.value::string AS cx
FROM PID_ONE_V p, LATERAL FLATTEN(INPUT => SPLIT(p.patient_id_list,'~')) f;

CREATE OR REPLACE TEMP VIEW PID_BEST_V AS
SELECT MESSAGE_ID,
       SPLIT_PART(cx,'^',1) AS id_number,
       SPLIT_PART(cx,'^',4) AS assigning_authority,
       SPLIT_PART(cx,'^',5) AS identifier_type_code,
       SPLIT_PART(cx,'^',6) AS assigning_facility
FROM PID_IDS_V
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY MESSAGE_ID
  ORDER BY CASE WHEN identifier_type_code IN ('MR','PI','RI') THEN 1 ELSE 9 END,
           NVL(assigning_authority,'') DESC
) = 1;

--------------------------------------------------------------------------------
-- 6) SEGMENTS (NO STREAMS/NO TASKS): Create table + full refresh INSERT … SELECT
--------------------------------------------------------------------------------

/*========================
  PID (Patient Identification)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_PID (
  MESSAGE_ID        STRING,
  FILENAME          STRING,
  FILE_ROW_NUMBER   NUMBER,
  PROCESS_TS        TIMESTAMP_NTZ,
  patient_id_list   STRING,
  patient_name_raw  STRING,
  dob_raw           STRING,
  sex               STRING,
  address_raw       STRING,
  last_name         STRING,
  first_name        STRING,
  middle_name       STRING,
  dob_date          DATE,
  dob_ts_ntz        TIMESTAMP_NTZ,
  dob_ts_tz         TIMESTAMP_TZ,
  street_1          STRING,
  street_2          STRING,
  city              STRING,
  state             STRING,
  postal_code       STRING,
  country           STRING,
  patient_id        STRING,
  patient_id_type   STRING,
  patient_id_auth   STRING,
  patient_id_facility STRING
);

TRUNCATE TABLE HL7_BRONZE_PID;
INSERT INTO HL7_BRONZE_PID
WITH base AS (
  SELECT
    MESSAGE_ID, FILENAME, FILE_ROW_NUMBER, PROCESS_TS, LINE,
    SPLIT_PART(LINE, '|', 4)  AS patient_id_list,  -- PID-3
    SPLIT_PART(LINE, '|', 6)  AS patient_name_raw, -- PID-5
    SPLIT_PART(LINE, '|', 8)  AS dob_raw,          -- PID-7
    SPLIT_PART(LINE, '|', 9)  AS sex,              -- PID-8
    SPLIT_PART(LINE, '|', 12) AS address_raw       -- PID-11
  FROM HL7_BRONZE_BASE
  WHERE SEGMENT_TYPE = 'PID'
)
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  b.patient_id_list, b.patient_name_raw, b.dob_raw, b.sex, b.address_raw,
  SPLIT_PART(b.patient_name_raw, '^', 1) AS last_name,
  SPLIT_PART(b.patient_name_raw, '^', 2) AS first_name,
  SPLIT_PART(b.patient_name_raw, '^', 3) AS middle_name,
  TRY_TO_DATE(SUBSTR(b.dob_raw, 1, 8), 'YYYYMMDD') AS dob_date,
  COALESCE(
    TRY_TO_TIMESTAMP_NTZ(SUBSTR(b.dob_raw, 1, 14), 'YYYYMMDDHH24MISS'),
    TRY_TO_TIMESTAMP_NTZ(SUBSTR(b.dob_raw, 1, 8),  'YYYYMMDD')
  ) AS dob_ts_ntz,
  CASE
    WHEN b.dob_raw REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(b.dob_raw, '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN b.dob_raw REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(b.dob_raw, 1, 14), 'YYYYMMDDHH24MISS')
    WHEN b.dob_raw REGEXP '^[0-9]{8}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(b.dob_raw, 1, 8), 'YYYYMMDD')
    ELSE NULL
  END AS dob_ts_tz,
  SPLIT_PART(b.address_raw, '^', 1) AS street_1,
  SPLIT_PART(b.address_raw, '^', 2) AS street_2,
  SPLIT_PART(b.address_raw, '^', 3) AS city,
  SPLIT_PART(b.address_raw, '^', 4) AS state,
  SPLIT_PART(b.address_raw, '^', 5) AS postal_code,
  SPLIT_PART(b.address_raw, '^', 6) AS country,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM base b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID);

/*========================
  EVN (Event)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_EVN (
  MESSAGE_ID        STRING,
  FILENAME          STRING,
  FILE_ROW_NUMBER   NUMBER,
  PROCESS_TS        TIMESTAMP_NTZ,
  event_type_code   STRING,
  recorded_dt_raw   STRING,
  recorded_dt_tz    TIMESTAMP_TZ,
  planned_dt_raw    STRING,
  planned_dt_tz     TIMESTAMP_TZ,
  event_reason_code STRING,
  operator_id_raw   STRING,
  event_occurred_raw STRING,
  event_occurred_tz  TIMESTAMP_TZ,
  event_facility     STRING,
  patient_id         STRING,
  patient_id_type    STRING,
  patient_id_auth    STRING,
  patient_id_facility STRING
);

TRUNCATE TABLE HL7_BRONZE_EVN;
INSERT INTO HL7_BRONZE_EVN
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE, '|', 2) AS event_type_code,
  SPLIT_PART(b.LINE, '|', 3) AS recorded_dt_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|', 3) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|', 3), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|', 3) REGEXP '^[0-9]{14}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',3),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|', 3) REGEXP '^[0-9]{8}$'  THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',3),1,8),  'YYYYMMDD')
    ELSE NULL
  END AS recorded_dt_tz,
  SPLIT_PART(b.LINE, '|', 4) AS planned_dt_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|', 4) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|', 4), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|', 4) REGEXP '^[0-9]{14}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',4),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|', 4) REGEXP '^[0-9]{8}$'  THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',4),1,8),  'YYYYMMDD')
    ELSE NULL
  END AS planned_dt_tz,
  SPLIT_PART(b.LINE, '|', 5) AS event_reason_code,
  SPLIT_PART(b.LINE, '|', 6) AS operator_id_raw,
  SPLIT_PART(b.LINE, '|', 7) AS event_occurred_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|', 7) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|', 7), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|', 7) REGEXP '^[0-9]{14}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',7),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|', 7) REGEXP '^[0-9]{8}$'  THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',7),1,8),  'YYYYMMDD')
    ELSE NULL
  END AS event_occurred_tz,
  SPLIT_PART(b.LINE, '|', 8) AS event_facility,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'EVN';


/*========================
  AL1 (Allergy) — robust INSERT (no ambiguous columns)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_AL1 (
  MESSAGE_ID            STRING,
  FILENAME              STRING,
  FILE_ROW_NUMBER       NUMBER,
  PROCESS_TS            TIMESTAMP_NTZ,
  set_id                STRING,
  allergy_type_raw      STRING,
  allergy_type_code     STRING,
  allergy_type_desc     STRING,
  allergen_raw          STRING,
  allergen_code         STRING,
  allergen_text         STRING,
  allergen_code_system  STRING,
  severity_raw          STRING,
  severity_code         STRING,
  severity_desc         STRING,
  reaction_raw          STRING,
  reaction_first        STRING,
  reaction_list         STRING,
  id_dt_raw             STRING,
  id_dt_tz              TIMESTAMP_TZ,
  id_dt_ntz             TIMESTAMP_NTZ,
  id_dt_date            DATE,
  patient_id_list       STRING,
  patient_id            STRING,
  patient_id_type       STRING,
  patient_id_auth       STRING,
  patient_id_facility   STRING
);

TRUNCATE TABLE HL7_BRONZE_AL1;

INSERT INTO HL7_BRONZE_AL1 (
  MESSAGE_ID, FILENAME, FILE_ROW_NUMBER, PROCESS_TS,
  set_id, allergy_type_raw, allergy_type_code, allergy_type_desc,
  allergen_raw, allergen_code, allergen_text, allergen_code_system,
  severity_raw, severity_code, severity_desc,
  reaction_raw, reaction_first, reaction_list,
  id_dt_raw, id_dt_tz, id_dt_ntz, id_dt_date,
  patient_id_list, patient_id, patient_id_type, patient_id_auth, patient_id_facility
)
WITH a AS (
  SELECT
    hb.MESSAGE_ID,
    hb.FILENAME,
    hb.FILE_ROW_NUMBER,
    hb.PROCESS_TS,
    hb.LINE,
    SPLIT_PART(hb.LINE, '|', 2) AS set_id,
    SPLIT_PART(hb.LINE, '|', 3) AS allergy_type_raw,
    SPLIT_PART(hb.LINE, '|', 4) AS allergen_raw,
    SPLIT_PART(hb.LINE, '|', 5) AS severity_raw,
    SPLIT_PART(hb.LINE, '|', 6) AS reaction_raw,
    SPLIT_PART(hb.LINE, '|', 7) AS id_dt_raw
  FROM HL7_BRONZE_BASE hb
  WHERE hb.SEGMENT_TYPE = 'AL1'
),
reaction_flat AS (
  SELECT
    a.MESSAGE_ID       AS MESSAGE_ID,
    a.FILE_ROW_NUMBER  AS FILE_ROW_NUMBER,
    r.value::string    AS reaction_item
  FROM a, LATERAL FLATTEN(INPUT => SPLIT(COALESCE(a.reaction_raw, ''), '~')) r
),
reaction_agg AS (
  SELECT
    rf.MESSAGE_ID,
    rf.FILE_ROW_NUMBER,
    MIN(rf.reaction_item) AS reaction_first,
    LISTAGG(rf.reaction_item, ', ') WITHIN GROUP (ORDER BY rf.reaction_item) AS reaction_list
  FROM reaction_flat rf
  GROUP BY rf.MESSAGE_ID, rf.FILE_ROW_NUMBER
)
SELECT
  a.MESSAGE_ID, a.FILENAME, a.FILE_ROW_NUMBER, a.PROCESS_TS,
  a.set_id,
  a.allergy_type_raw,
  SPLIT_PART(a.allergy_type_raw, '^', 1) AS allergy_type_code,
  SPLIT_PART(a.allergy_type_raw, '^', 2) AS allergy_type_desc,
  a.allergen_raw,
  SPLIT_PART(a.allergen_raw, '^', 1) AS allergen_code,
  SPLIT_PART(a.allergen_raw, '^', 2) AS allergen_text,
  SPLIT_PART(a.allergen_raw, '^', 3) AS allergen_code_system,
  a.severity_raw,
  SPLIT_PART(a.severity_raw, '^', 1) AS severity_code,
  SPLIT_PART(a.severity_raw, '^', 2) AS severity_desc,
  a.reaction_raw,
  r.reaction_first,
  r.reaction_list,
  a.id_dt_raw,
  CASE
    WHEN a.id_dt_raw REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(a.id_dt_raw, '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN a.id_dt_raw REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(a.id_dt_raw, 1, 14), 'YYYYMMDDHH24MISS')
    WHEN a.id_dt_raw REGEXP '^[0-9]{8}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(a.id_dt_raw, 1, 8), 'YYYYMMDD')
    ELSE NULL
  END AS id_dt_tz,
  TRY_TO_TIMESTAMP_NTZ(
    CASE WHEN a.id_dt_raw REGEXP '^[0-9]{14}' THEN SUBSTR(a.id_dt_raw, 1, 14)
         WHEN a.id_dt_raw REGEXP '^[0-9]{8}'  THEN SUBSTR(a.id_dt_raw, 1, 8)
         ELSE NULL END,
    CASE WHEN a.id_dt_raw REGEXP '^[0-9]{14}' THEN 'YYYYMMDDHH24MISS'
         WHEN a.id_dt_raw REGEXP '^[0-9]{8}'  THEN 'YYYYMMDD'
         ELSE NULL END
  ) AS id_dt_ntz,
  TRY_TO_DATE(
    CASE WHEN a.id_dt_raw REGEXP '^[0-9]{14}' THEN SUBSTR(a.id_dt_raw, 1, 8)
         WHEN a.id_dt_raw REGEXP '^[0-9]{8}'  THEN SUBSTR(a.id_dt_raw, 1, 8)
         ELSE NULL END, 'YYYYMMDD'
  ) AS id_dt_date,
  p.patient_id_list,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM a
LEFT JOIN reaction_agg r
  ON r.MESSAGE_ID = a.MESSAGE_ID
 AND r.FILE_ROW_NUMBER = a.FILE_ROW_NUMBER
LEFT JOIN PID_ID_LIST_V p
  ON p.MESSAGE_ID = a.MESSAGE_ID
LEFT JOIN PID_BEST_V pb
  ON pb.MESSAGE_ID = a.MESSAGE_ID;


/*========================
  PV1 (Visit)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_PV1 (
  MESSAGE_ID             STRING,
  FILENAME               STRING,
  FILE_ROW_NUMBER        NUMBER,
  PROCESS_TS             TIMESTAMP_NTZ,
  set_id                 STRING,
  patient_class          STRING,
  assigned_location_raw  STRING,
  admission_type         STRING,
  preadmit_number        STRING,
  prior_location_raw     STRING,
  attending_raw          STRING,
  referring_raw          STRING,
  consulting_raw         STRING,
  hospital_service_raw   STRING,
  vip_indicator          STRING,
  admitting_raw          STRING,
  patient_type           STRING,
  visit_number_raw       STRING,
  admit_dt_raw           STRING,
  discharge_dt_raw       STRING,
  loc_point_of_care      STRING,
  loc_room_raw           STRING,
  loc_room               STRING,
  loc_room_numeric       NUMBER,
  loc_facility           STRING,
  loc_building           STRING,
  loc_floor              STRING,
  loc_description        STRING,
  prior_point_of_care    STRING,
  prior_room             STRING,
  prior_facility         STRING,
  attending_id           STRING,
  attending_last_name    STRING,
  attending_first_name   STRING,
  referring_id           STRING,
  referring_last_name    STRING,
  referring_first_name   STRING,
  consulting_id          STRING,
  consulting_last_name   STRING,
  consulting_first_name  STRING,
  admitting_id           STRING,
  admitting_last_name    STRING,
  admitting_first_name   STRING,
  hospital_service_code  STRING,
  hospital_service_desc  STRING,
  visit_number           STRING,
  visit_assigning_authority STRING,
  admit_dt_tz            TIMESTAMP_TZ,
  discharge_dt_tz        TIMESTAMP_TZ,
  patient_id             STRING,
  patient_id_type        STRING,
  patient_id_auth        STRING,
  patient_id_facility    STRING
);

TRUNCATE TABLE HL7_BRONZE_PV1;
INSERT INTO HL7_BRONZE_PV1
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE, '|', 2)  AS set_id,
  SPLIT_PART(b.LINE, '|', 3)  AS patient_class,
  SPLIT_PART(b.LINE, '|', 4)  AS assigned_location_raw,
  SPLIT_PART(b.LINE, '|', 5)  AS admission_type,
  SPLIT_PART(b.LINE, '|', 6)  AS preadmit_number,
  SPLIT_PART(b.LINE, '|', 7)  AS prior_location_raw,
  SPLIT_PART(b.LINE, '|', 8)  AS attending_raw,
  SPLIT_PART(b.LINE, '|', 9)  AS referring_raw,
  SPLIT_PART(b.LINE, '|', 10) AS consulting_raw,
  SPLIT_PART(b.LINE, '|', 11) AS hospital_service_raw,
  SPLIT_PART(b.LINE, '|', 16) AS vip_indicator,
  SPLIT_PART(b.LINE, '|', 17) AS admitting_raw,
  SPLIT_PART(b.LINE, '|', 19) AS patient_type,
  SPLIT_PART(b.LINE, '|', 20) AS visit_number_raw,
  SPLIT_PART(b.LINE, '|', 45) AS admit_dt_raw,
  SPLIT_PART(b.LINE, '|', 46) AS discharge_dt_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 1) AS loc_point_of_care,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 2) AS loc_room_raw,
  NULLIF(REGEXP_REPLACE(SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 2), '^[\\-\\s]+', ''), '') AS loc_room,
  TRY_TO_NUMBER(NULLIF(REGEXP_REPLACE(SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 2), '^[\\-\\s]+', ''), '')) AS loc_room_numeric,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 4) AS loc_facility,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 7) AS loc_building,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 8) AS loc_floor,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4), '^', 9) AS loc_description,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 7), '^', 1) AS prior_point_of_care,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 7), '^', 2) AS prior_room,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 7), '^', 4) AS prior_facility,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 8), '^', 1) AS attending_id,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 8), '^', 2) AS attending_last_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 8), '^', 3) AS attending_first_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 9), '^', 1) AS referring_id,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 9), '^', 2) AS referring_last_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 9), '^', 3) AS referring_first_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',10), '^', 1) AS consulting_id,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',10), '^', 2) AS consulting_last_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',10), '^', 3) AS consulting_first_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',17), '^', 1) AS admitting_id,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',17), '^', 2) AS admitting_last_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',17), '^', 3) AS admitting_first_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',11), '^', 1) AS hospital_service_code,
  INITCAP(TRIM(SPLIT_PART(SPLIT_PART(b.LINE, '|',11), '^', 2))) AS hospital_service_desc,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',20), '^', 1) AS visit_number,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',20), '^', 4) AS visit_assigning_authority,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',45) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',45), '(\\+\\-?[0-9]{4})$', ' \\1'),'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',45) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',45),1,14),'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',45) REGEXP '^[0-9]{8}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',45),1,8),'YYYYMMDD')
    ELSE NULL
  END AS admit_dt_tz,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',46) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',46), '(\\+\\-?[0-9]{4})$', ' \\1'),'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',46) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',46),1,14),'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',46) REGEXP '^[0-9]{8}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',46),1,8),'YYYYMMDD')
    ELSE NULL
  END AS discharge_dt_tz,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'PV1';

/*========================
  IN1 (Insurance)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_IN1 (
  MESSAGE_ID              STRING,
  FILENAME                STRING,
  FILE_ROW_NUMBER         NUMBER,
  PROCESS_TS              TIMESTAMP_NTZ,
  set_id                  STRING,
  plan_id_raw             STRING,
  plan_id_code            STRING,
  plan_id_desc            STRING,
  company_id_raw          STRING,
  company_id_code         STRING,
  company_id_desc         STRING,
  company_name            STRING,
  company_address_raw     STRING,
  company_street_1        STRING,
  company_street_2        STRING,
  company_city            STRING,
  company_state           STRING,
  company_postal          STRING,
  company_contact_person  STRING,
  company_phone           STRING,
  group_name              STRING,
  plan_type_raw           STRING,
  plan_type_code          STRING,
  plan_type_desc          STRING,
  insured_name_raw        STRING,
  insured_last_name       STRING,
  insured_first_name      STRING,
  insured_middle_name     STRING,
  insured_relationship    STRING,
  insured_dob_raw         STRING,
  insured_dob_tz          TIMESTAMP_TZ,
  insured_address_raw     STRING,
  insured_street_1        STRING,
  insured_street_2        STRING,
  insured_city            STRING,
  insured_state           STRING,
  insured_postal          STRING,
  assign_benefits         STRING,
  coordination_of_benefits STRING,
  notice_admission_dt_raw STRING,
  notice_admission_dt_tz  TIMESTAMP_TZ,
  patient_id              STRING,
  patient_id_type         STRING,
  patient_id_auth         STRING,
  patient_id_facility     STRING
);

TRUNCATE TABLE HL7_BRONZE_IN1;
INSERT INTO HL7_BRONZE_IN1
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE, '|', 2)  AS set_id,
  SPLIT_PART(b.LINE, '|', 3)  AS plan_id_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 3),'^',1) AS plan_id_code,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 3),'^',2) AS plan_id_desc,
  SPLIT_PART(b.LINE, '|', 4)  AS company_id_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4),'^',1) AS company_id_code,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 4),'^',2) AS company_id_desc,
  SPLIT_PART(b.LINE, '|', 5)  AS company_name,
  SPLIT_PART(b.LINE, '|', 6)  AS company_address_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 6),'^',1) AS company_street_1,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 6),'^',2) AS company_street_2,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 6),'^',3) AS company_city,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 6),'^',4) AS company_state,
  SPLIT_PART(SPLIT_PART(b.LINE, '|', 6),'^',5) AS company_postal,
  SPLIT_PART(b.LINE, '|', 7)  AS company_contact_person,
  SPLIT_PART(b.LINE, '|', 8)  AS company_phone,
  SPLIT_PART(b.LINE, '|', 11) AS group_name,
  SPLIT_PART(b.LINE, '|', 16) AS plan_type_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',16),'^',1) AS plan_type_code,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',16),'^',2) AS plan_type_desc,
  SPLIT_PART(b.LINE, '|', 17) AS insured_name_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',17),'^',1) AS insured_last_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',17),'^',2) AS insured_first_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',17),'^',3) AS insured_middle_name,
  SPLIT_PART(b.LINE, '|', 18) AS insured_relationship,
  SPLIT_PART(b.LINE, '|', 19) AS insured_dob_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',19) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',19), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',19) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',19),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',19) REGEXP '^[0-9]{8}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',19),1,8), 'YYYYMMDD')
    ELSE NULL
  END AS insured_dob_tz,
  SPLIT_PART(b.LINE, '|', 20) AS insured_address_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',20),'^',1) AS insured_street_1,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',20),'^',2) AS insured_street_2,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',20),'^',3) AS insured_city,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',20),'^',4) AS insured_state,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',20),'^',5) AS insured_postal,
  SPLIT_PART(b.LINE, '|', 21) AS assign_benefits,
  SPLIT_PART(b.LINE, '|', 22) AS coordination_of_benefits,
  SPLIT_PART(b.LINE, '|', 25) AS notice_admission_dt_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',25) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',25), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',25) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',25),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',25) REGEXP '^[0-9]{8}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',25),1,8), 'YYYYMMDD')
    ELSE NULL
  END AS notice_admission_dt_tz,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'IN1';

/*========================
  GT1 (Guarantor)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_GT1 (
  MESSAGE_ID              STRING,
  FILENAME                STRING,
  FILE_ROW_NUMBER         NUMBER,
  PROCESS_TS              TIMESTAMP_NTZ,
  set_id                  STRING,
  guarantor_name_raw      STRING,
  guarantor_last_name     STRING,
  guarantor_first_name    STRING,
  guarantor_middle_name   STRING,
  guarantor_address_raw   STRING,
  guarantor_street_1      STRING,
  guarantor_street_2      STRING,
  guarantor_city          STRING,
  guarantor_state         STRING,
  guarantor_postal        STRING,
  guarantor_phone_home    STRING,
  guarantor_phone_business STRING,
  guarantor_dob_raw       STRING,
  guarantor_dob_tz        TIMESTAMP_TZ,
  guarantor_sex           STRING,
  guarantor_relationship  STRING,
  guarantor_ssn           STRING,
  patient_id              STRING,
  patient_id_type         STRING,
  patient_id_auth         STRING,
  patient_id_facility     STRING
);

TRUNCATE TABLE HL7_BRONZE_GT1;
INSERT INTO HL7_BRONZE_GT1
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE, '|', 2)  AS set_id,
  SPLIT_PART(b.LINE, '|', 4)  AS guarantor_name_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',4),'^',1) AS guarantor_last_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',4),'^',2) AS guarantor_first_name,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',4),'^',3) AS guarantor_middle_name,
  SPLIT_PART(b.LINE, '|', 6)  AS guarantor_address_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',6),'^',1) AS guarantor_street_1,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',6),'^',2) AS guarantor_street_2,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',6),'^',3) AS guarantor_city,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',6),'^',4) AS guarantor_state,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',6),'^',5) AS guarantor_postal,
  SPLIT_PART(b.LINE, '|', 7)  AS guarantor_phone_home,
  SPLIT_PART(b.LINE, '|', 8)  AS guarantor_phone_business,
  SPLIT_PART(b.LINE, '|', 9)  AS guarantor_dob_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',9) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',9), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',9) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',9),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',9) REGEXP '^[0-9]{8}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',9),1,8), 'YYYYMMDD')
    ELSE NULL
  END AS guarantor_dob_tz,
  SPLIT_PART(b.LINE, '|', 10) AS guarantor_sex,
  SPLIT_PART(b.LINE, '|', 12) AS guarantor_relationship,
  SPLIT_PART(b.LINE, '|', 15) AS guarantor_ssn,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'GT1';

/*========================
  DG1 (Diagnosis)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_DG1 (
  MESSAGE_ID         STRING,
  FILENAME           STRING,
  FILE_ROW_NUMBER    NUMBER,
  PROCESS_TS         TIMESTAMP_NTZ,
  set_id             STRING,
  coding_method      STRING,
  diagnosis_raw      STRING,
  diagnosis_code     STRING,
  diagnosis_text     STRING,
  diagnosis_system   STRING,
  diagnosis_desc     STRING,
  diagnosis_dt_raw   STRING,
  diagnosis_dt_tz    TIMESTAMP_TZ,
  diagnosis_type     STRING,
  patient_id         STRING,
  patient_id_type    STRING,
  patient_id_auth    STRING,
  patient_id_facility STRING
);

TRUNCATE TABLE HL7_BRONZE_DG1;
INSERT INTO HL7_BRONZE_DG1
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE, '|', 2) AS set_id,
  SPLIT_PART(b.LINE, '|', 3) AS coding_method,
  SPLIT_PART(b.LINE, '|', 4) AS diagnosis_raw,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',4),'^',1) AS diagnosis_code,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',4),'^',2) AS diagnosis_text,
  SPLIT_PART(SPLIT_PART(b.LINE, '|',4),'^',3) AS diagnosis_system,
  SPLIT_PART(b.LINE, '|', 5) AS diagnosis_desc,
  SPLIT_PART(b.LINE, '|', 7) AS diagnosis_dt_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',7) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',7), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',7) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',7),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',7) REGEXP '^[0-9]{8}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',7),1,8), 'YYYYMMDD')
    ELSE NULL
  END AS diagnosis_dt_tz,
  SPLIT_PART(b.LINE, '|', 8) AS diagnosis_type,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'DG1';

/*========================
  ORC (Common Order)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_ORC (
  MESSAGE_ID            STRING,
  FILENAME              STRING,
  FILE_ROW_NUMBER       NUMBER,
  PROCESS_TS            TIMESTAMP_NTZ,
  order_control         STRING,  -- ORC-1
  placer_order_number   STRING,  -- ORC-2
  filler_order_number   STRING,  -- ORC-3
  order_status          STRING,  -- ORC-5
  transaction_dt_raw    STRING,  -- ORC-9
  transaction_dt_tz     TIMESTAMP_TZ,
  ordering_provider_raw STRING,  -- ORC-12 (XCN)
  ordering_provider_id  STRING,
  ordering_provider_last STRING,
  ordering_provider_first STRING,
  patient_id            STRING,
  patient_id_type       STRING,
  patient_id_auth       STRING,
  patient_id_facility   STRING
);

TRUNCATE TABLE HL7_BRONZE_ORC;
INSERT INTO HL7_BRONZE_ORC
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE,'|',1) AS order_control,
  SPLIT_PART(b.LINE,'|',2) AS placer_order_number,
  SPLIT_PART(b.LINE,'|',3) AS filler_order_number,
  SPLIT_PART(b.LINE,'|',5) AS order_status,
  SPLIT_PART(b.LINE,'|',9) AS transaction_dt_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',9) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',9), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',9) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',9),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',9) REGEXP '^[0-9]{8}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',9),1,8),'YYYYMMDD')
    ELSE NULL
  END AS transaction_dt_tz,
  SPLIT_PART(b.LINE,'|',12) AS ordering_provider_raw,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',12),'^',1) AS ordering_provider_id,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',12),'^',2) AS ordering_provider_last,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',12),'^',3) AS ordering_provider_first,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'ORC';

/*========================
  OBR (Observation Request)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_OBR (
  MESSAGE_ID              STRING,
  FILENAME                STRING,
  FILE_ROW_NUMBER         NUMBER,
  PROCESS_TS              TIMESTAMP_NTZ,
  set_id                  STRING,  -- OBR-1
  placer_order_number     STRING,  -- OBR-2
  filler_order_number     STRING,  -- OBR-3
  usi_raw                 STRING,  -- OBR-4
  usi_code                STRING,
  usi_text                STRING,
  usi_system              STRING,
  priority                STRING,  -- OBR-5
  observation_dt_raw      STRING,  -- OBR-7
  observation_dt_tz       TIMESTAMP_TZ,
  result_status           STRING,  -- OBR-25
  patient_id              STRING,
  patient_id_type         STRING,
  patient_id_auth         STRING,
  patient_id_facility     STRING
);

TRUNCATE TABLE HL7_BRONZE_OBR;
INSERT INTO HL7_BRONZE_OBR
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE,'|',1) AS set_id,
  SPLIT_PART(b.LINE,'|',2) AS placer_order_number,
  SPLIT_PART(b.LINE,'|',3) AS filler_order_number,
  SPLIT_PART(b.LINE,'|',4) AS usi_raw,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',4),'^',1) AS usi_code,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',4),'^',2) AS usi_text,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',4),'^',3) AS usi_system,
  SPLIT_PART(b.LINE,'|',5) AS priority,
  SPLIT_PART(b.LINE,'|',7) AS observation_dt_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',7) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',7), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',7) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',7),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',7) REGEXP '^[0-9]{8}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',7),1,8),'YYYYMMDD')
    ELSE NULL
  END AS observation_dt_tz,
  SPLIT_PART(b.LINE,'|',25) AS result_status,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'OBR';

/*========================
  OBX (Observation Result)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_OBX (
  MESSAGE_ID          STRING,
  FILENAME            STRING,
  FILE_ROW_NUMBER     NUMBER,
  PROCESS_TS          TIMESTAMP_NTZ,
  set_id              STRING,  -- OBX-1
  value_type          STRING,  -- OBX-2
  observation_id_raw  STRING,  -- OBX-3
  observation_id_code STRING,
  observation_id_text STRING,
  observation_id_system STRING,
  observation_value   STRING,  -- OBX-5
  units_raw           STRING,  -- OBX-6
  units_code          STRING,
  units_text          STRING,
  reference_range     STRING,  -- OBX-7
  abnormal_flags      STRING,  -- OBX-8
  result_status       STRING,  -- OBX-11
  observation_dt_raw  STRING,  -- OBX-14
  observation_dt_tz   TIMESTAMP_TZ,
  patient_id          STRING,
  patient_id_type     STRING,
  patient_id_auth     STRING,
  patient_id_facility STRING
);

TRUNCATE TABLE HL7_BRONZE_OBX;
INSERT INTO HL7_BRONZE_OBX
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE,'|',1) AS set_id,
  SPLIT_PART(b.LINE,'|',2) AS value_type,
  SPLIT_PART(b.LINE,'|',3) AS observation_id_raw,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',3),'^',1) AS observation_id_code,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',3),'^',2) AS observation_id_text,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',3),'^',3) AS observation_id_system,
  SPLIT_PART(b.LINE,'|',5) AS observation_value,
  SPLIT_PART(b.LINE,'|',6) AS units_raw,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',6),'^',1) AS units_code,
  SPLIT_PART(SPLIT_PART(b.LINE,'|',6),'^',2) AS units_text,
  SPLIT_PART(b.LINE,'|',7) AS reference_range,
  SPLIT_PART(b.LINE,'|',8) AS abnormal_flags,
  SPLIT_PART(b.LINE,'|',11) AS result_status,
  SPLIT_PART(b.LINE,'|',14) AS observation_dt_raw,
  CASE
    WHEN SPLIT_PART(b.LINE, '|',14) REGEXP '^[0-9]{14}[\\+\\-][0-9]{4}$' THEN
      TRY_TO_TIMESTAMP_TZ(REGEXP_REPLACE(SPLIT_PART(b.LINE, '|',14), '(\\+\\-?[0-9]{4})$', ' \\1'), 'YYYYMMDDHH24MISS TZHTZM')
    WHEN SPLIT_PART(b.LINE, '|',14) REGEXP '^[0-9]{14}$' THEN
      TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',14),1,14), 'YYYYMMDDHH24MISS')
    WHEN SPLIT_PART(b.LINE, '|',14) REGEXP '^[0-9]{8}$' THEN TRY_TO_TIMESTAMP_TZ(SUBSTR(SPLIT_PART(b.LINE, '|',14),1,8),'YYYYMMDD')
    ELSE NULL
  END AS observation_dt_tz,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'OBX';

/*========================
  NTE (Notes)
========================*/
CREATE OR REPLACE TABLE HL7_BRONZE_NTE (
  MESSAGE_ID        STRING,
  FILENAME          STRING,
  FILE_ROW_NUMBER   NUMBER,
  PROCESS_TS        TIMESTAMP_NTZ,
  set_id            STRING,  -- NTE-1
  source            STRING,  -- NTE-2
  comment           STRING,  -- NTE-3
  patient_id        STRING,
  patient_id_type   STRING,
  patient_id_auth   STRING,
  patient_id_facility STRING
);

TRUNCATE TABLE HL7_BRONZE_NTE;
INSERT INTO HL7_BRONZE_NTE
SELECT
  b.MESSAGE_ID, b.FILENAME, b.FILE_ROW_NUMBER, b.PROCESS_TS,
  SPLIT_PART(b.LINE,'|',1) AS set_id,
  SPLIT_PART(b.LINE,'|',2) AS source,
  SPLIT_PART(b.LINE,'|',3) AS comment,
  pb.id_number            AS patient_id,
  pb.identifier_type_code AS patient_id_type,
  pb.assigning_authority  AS patient_id_auth,
  pb.assigning_facility   AS patient_id_facility
FROM HL7_BRONZE_BASE b
LEFT JOIN PID_BEST_V pb USING (MESSAGE_ID)
WHERE b.SEGMENT_TYPE = 'NTE';

-- Done. Re-run the two refresh steps when new data arrives:
-- 1) COPY INTO HL7_BRONZE_RAW ... FROM @HL7_STAGE ...
-- 2) TRUNCATE TABLE HL7_BRONZE_BASE; INSERT INTO HL7_BRONZE_BASE SELECT ... FROM HL7_BRONZE_BASE_V;
-- Then re-run the TRUNCATE+INSERT blocks for each segment as needed.

