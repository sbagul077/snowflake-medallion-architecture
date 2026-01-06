
-- Consolidated immunization union table (revised) with both DATE--
CREATE OR REPLACE TABLE FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION (
  Patient_id       STRING,
  Patient_name     STRING,
  Gender           STRING,
  City             STRING,
  State            STRING,
  Encounter_id     STRING,
  Imm_date         DATE,            -- canonical date (good for dashboards)
  Imm_ts           TIMESTAMP_NTZ,   -- optional precise timestamp when available
  Code             STRING,
  Code_system      STRING,
  Description      STRING,
  Organization_id  STRING,
  Provider_id      STRING
);

--Inserting CSV → IMMUNIZATION_UNION
INSERT INTO FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION (
  patient_id,
  encounter_id,
  imm_date,
  imm_ts,
  code,
  code_system,
  description,
  organization_id,
  provider_id,
  gender,
  city,
  state,
  patient_name
)
SELECT
  COALESCE(NULLIF(imm.PATIENT, ''), 'Unknown')                    AS patient_id,
  COALESCE(NULLIF(imm.ENCOUNTER, ''), 'Unknown')                  AS encounter_id,

  /* DATE-only for Power BI dim_date */
  TRY_TO_DATE(imm.DATE)                                           AS imm_date,

  /* CSV has no timestamp -> NULL */
  NULL                                                            AS imm_ts,

  COALESCE(NULLIF(imm.CODE, ''), 'Unknown')                       AS code,
  'Unknown'                                                       AS code_system,  -- CSV has no code_system
  COALESCE(NULLIF(imm.DESCRIPTION, ''), 'Unknown')                AS description,

  COALESCE(NULLIF(enc.ORGANIZATION, ''), 'Unknown')               AS organization_id,
  COALESCE(NULLIF(enc.PROVIDER, ''), 'Unknown')                   AS provider_id,

  COALESCE(NULLIF(pat.GENDER, ''), 'Unknown')                     AS gender,
  COALESCE(NULLIF(pat.CITY, ''), 'Unknown')                       AS city,
  COALESCE(NULLIF(pat.STATE, ''), 'Unknown')                      AS state,

  CASE
    WHEN TRIM(CONCAT(COALESCE(pat.FIRST, ''), ' ', COALESCE(pat.LAST, ''))) <> ''
      THEN TRIM(CONCAT(COALESCE(pat.FIRST, ''), ' ', COALESCE(pat.LAST, '')))
    ELSE 'Unknown'
  END                                                             AS patient_name

FROM FINAL_ASSIGNMENT.CSV.IMMUNIZATIONS  AS imm
LEFT JOIN FINAL_ASSIGNMENT.CSV.PATIENTS  AS pat ON pat.Id = imm.PATIENT
LEFT JOIN FINAL_ASSIGNMENT.CSV.ENCOUNTERS AS enc ON enc.Id = imm.ENCOUNTER;

--inserting ccda immunization into union

INSERT INTO FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION (
  patient_id,
  encounter_id,
  imm_date,
  imm_ts,
  code,
  code_system,
  description,
  organization_id,
  provider_id,
  gender,
  city,
  state,
  patient_name
)
SELECT
  COALESCE(NULLIF(cc.PATIENT_ID,''), 'Unknown')   AS patient_id,
  'Unknown'                                       AS encounter_id,

  /* DATE-only extracted from START_RAW */
  CAST(
    CASE
      WHEN cc.START_RAW IS NULL OR cc.START_RAW = '' THEN NULL
      WHEN LENGTH(cc.START_RAW) = 14
        THEN TRY_TO_TIMESTAMP_NTZ(cc.START_RAW, 'YYYYMMDDHH24MISS')
      WHEN LENGTH(cc.START_RAW) = 12
        THEN TRY_TO_TIMESTAMP_NTZ(cc.START_RAW, 'YYYYMMDDHH24MI')
      WHEN LENGTH(cc.START_RAW) = 8
        THEN TRY_TO_TIMESTAMP_NTZ(cc.START_RAW, 'YYYYMMDD')
      ELSE NULL
    END
  AS DATE)                                         AS imm_date,

  /* Per your rule: keep CCDA timestamp empty */
  NULL                                             AS imm_ts,

  COALESCE(NULLIF(cc.CODE,''), 'Unknown')          AS code,
  COALESCE(NULLIF(cc.CODE_SYSTEM,''), 'Unknown')   AS code_system,
  COALESCE(NULLIF(cc.DESCRIPTION,''), 'Unknown')   AS description,

  /* CCDA lacks these -> 'Unknown' */
  'Unknown'                                        AS organization_id,
  'Unknown'                                        AS provider_id,
  'Unknown'                                        AS gender,
  'Unknown'                                        AS city,
  'Unknown'                                        AS state,

  /* No patient name in CCDA -> 'Unknown' */
  'Unknown'                                        AS patient_name

FROM FINAL_ASSIGNMENT.CCDA.CCDA_IMMUNIZATIONS cc;

--display imm_union_table
select * from FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION;

-- Fastest: clears all rows, resets metadata
TRUNCATE TABLE FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION;


select * from FINAL_ASSIGNMENT.CCDA.CCDA_VITALS;

select * from FINAL_ASSIGNMENT.ccda.CCDA_immunizations;



select  * from FINAL_ASSIGNMENT.csv.allergies;

select  * from FINAL_ASSIGNMENT.csv.immunizations;