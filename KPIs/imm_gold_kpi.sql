

CREATE OR REPLACE TABLE FINAL_ASSIGNMENT.gold.dim_vaccine (
  code           STRING PRIMARY KEY,
  clinical_group STRING,
  vaccine_name   STRING
);

-- Build from distinct CVX codes present in IMMUNIZATION_UNION
--This uses distinct codes from your union (where codes like 140, 33, 133, 208 appear many times) and maps them to clean names/groups
INSERT INTO FINAL_ASSIGNMENT.gold.dim_vaccine (code, clinical_group, vaccine_name)
SELECT DISTINCT
  u.CODE,
  CASE u.CODE
    WHEN '140' THEN 'flu'
    WHEN '33'  THEN 'ppv'
    WHEN '133' THEN 'pcv13'
    WHEN '208' THEN 'covid19'
    WHEN '113' THEN 'td'
    WHEN '115' THEN 'tdap'
    WHEN '49'  THEN 'hib'
    WHEN '62'  THEN 'hpv'
    WHEN '52'  THEN 'hepA_adult'
    WHEN '83'  THEN 'hepA_ped'
    WHEN '43'  THEN 'hepB_adult'
    WHEN '08'  THEN 'hepB_ped'
    WHEN '03'  THEN 'mmr'
    WHEN '21'  THEN 'varicella'
    WHEN '10'  THEN 'ipv'
    WHEN '20'  THEN 'dtap'
    WHEN '119' THEN 'rotavirus'
    WHEN '114' THEN 'meningococcal'
    WHEN '121' THEN 'zoster'
    ELSE 'other'
  END AS clinical_group,
  CASE u.CODE
    WHEN '140' THEN 'Influenza seasonal injectable preservative free'
    WHEN '33'  THEN 'Pneumococcal polysaccharide vaccine, 23-valent'
    WHEN '133' THEN 'Pneumococcal conjugate PCV 13'
    WHEN '208' THEN 'COVID-19 mRNA (30 mcg/0.3mL)'
    WHEN '113' THEN 'Td (adult) 5 Lf tetanus toxoid PF adsorbed'
    WHEN '115' THEN 'Tdap'
    WHEN '49'  THEN 'Hib (PRP-OMP)'
    WHEN '62'  THEN 'HPV quadrivalent'
    WHEN '52'  THEN 'Hep A adult'
    WHEN '83'  THEN 'Hep A ped/adol 2 dose'
    WHEN '43'  THEN 'Hep B adult'
    WHEN '08'  THEN 'Hep B adolescent/pediatric'
    WHEN '03'  THEN 'MMR'
    WHEN '21'  THEN 'Varicella'
    WHEN '10'  THEN 'IPV'
    WHEN '20'  THEN 'DTaP'
    WHEN '119' THEN 'Rotavirus monovalent'
    WHEN '114' THEN 'Meningococcal MCV4P'
    WHEN '121' THEN 'Zoster vaccine live'
    ELSE COALESCE(NULLIF(u.DESCRIPTION,''),'Unknown')
  END AS vaccine_name
FROM FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION u;


select * from FINAL_ASSIGNMENT.gold.dim_vaccine;

--DIMENSION table_date

-- Create the table once
CREATE OR REPLACE TABLE FINAL_ASSIGNMENT.GOLD.dim_date (
  date        DATE PRIMARY KEY,
  year        INT,
  quarter     INT,
  month       INT,
  month_name  STRING,
  day         INT,
  day_name    STRING
);

-- Populate with the right span
INSERT INTO FINAL_ASSIGNMENT.GOLD.dim_date (date, year, quarter, month, month_name, day, day_name)
WITH bounds AS (
  SELECT MIN(IMM_DATE) AS min_d, MAX(IMM_DATE) AS max_d
  FROM FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION
),
gen AS (
  -- Use a large constant; filter later
  SELECT SEQ4() AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 200000))
)
SELECT
  DATEADD('day', gen.n, bounds.min_d)                            AS date,
  YEAR(DATEADD('day', gen.n, bounds.min_d))                      AS year,
  QUARTER(DATEADD('day', gen.n, bounds.min_d))                   AS quarter,
  MONTH(DATEADD('day', gen.n, bounds.min_d))                     AS month,
  TO_CHAR(DATEADD('day', gen.n, bounds.min_d), 'Mon')            AS month_name,
  DAY(DATEADD('day', gen.n, bounds.min_d))                       AS day,
  TO_CHAR(DATEADD('day', gen.n, bounds.min_d), 'DY')             AS day_name
FROM bounds
CROSS JOIN gen
WHERE gen.n <= DATEDIFF('day', bounds.min_d, bounds.max_d);

--display dim_date
select * from FINAL_ASSIGNMENT.GOLD.dim_date;

--display dim_vaccine
select * from FINAL_ASSIGNMENT.gold.dim_vaccine;


--dimension table - location
CREATE OR REPLACE TABLE FINAL_ASSIGNMENT.GOLDFINAL_ASSIGNMENT.gold.dim_location (
  location_key STRING PRIMARY KEY,
  city STRING,
  state STRING
);

--dim_location
INSERT INTO FINAL_ASSIGNMENT.gold.dim_location (location_key, city, state)
SELECT DISTINCT
  MD5(CONCAT_WS('|',
    COALESCE(NULLIF(CITY,''),'Unknown'),
    COALESCE(NULLIF(STATE,''),'Unknown'))),
  COALESCE(NULLIF(CITY,''),'Unknown'),
  COALESCE(NULLIF(STATE,''),'Unknown')
FROM FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION;

select * from FINAL_ASSIGNMENT.gold.dim_location; 

--Enriched Events DT (shared base for your domain/KPI)
CREATE OR REPLACE DYNAMIC TABLE FINAL_ASSIGNMENT.GOLD.dt_immunization_enriched
  TARGET_LAG = '5 MINUTES'
  WAREHOUSE  = COMPUTE_WH
AS
SELECT
  u.PATIENT_ID,
  u.PATIENT_NAME,
  COALESCE(NULLIF(u.GENDER,''), 'Unknown')          AS GENDER,
  COALESCE(NULLIF(u.CITY,''), 'Unknown')            AS CITY,
  COALESCE(NULLIF(u.STATE,''), 'Unknown')           AS STATE,
  COALESCE(NULLIF(u.ORGANIZATION_ID,''), 'Unknown') AS ORGANIZATION_ID,
  COALESCE(NULLIF(u.PROVIDER_ID,''), 'Unknown')     AS PROVIDER_ID,
  u.ENCOUNTER_ID,
  u.IMM_DATE,
  u.CODE,
  COALESCE(v.CLINICAL_GROUP, 'other')               AS CLINICAL_GROUP,
  COALESCE(v.VACCINE_NAME, COALESCE(NULLIF(u.DESCRIPTION,''),'Unknown')) AS VACCINE_NAME
FROM FINAL_ASSIGNMENT.SILVER.IMMUNIZATION_UNION u
LEFT JOIN FINAL_ASSIGNMENT.GOLD.dim_vaccine v
       ON v.code = u.code
WHERE u.IMM_DATE IS NOT NULL;

select * from FINAL_ASSIGNMENT.GOLD.dt_immunization_enriched;


-- ==========================================================================================
--KPI DT — Immunization Coverage (single table for your dashboard)
-- Inputs        : FINAL_ASSIGNMENT.GOLD.dim_date (calendar)
--                 FINAL_ASSIGNMENT.GOLD.dt_immunization_enriched (joined and standardized events)
-- Outputs       : Monthly & Ever-to-date numerators, denominators, coverage %, events count
-- ==========================================================================================
CREATE OR REPLACE DYNAMIC TABLE FINAL_ASSIGNMENT.GOLD.dt_kpi_immunization_coverage
  TARGET_LAG = '10 MINUTES'
  WAREHOUSE  = COMPUTE_WH
AS
/* -----------------------------
   1) Month windows from dim_date
   ----------------------------- */
WITH months AS (
  SELECT
    year,
    month,
    MIN(date) AS month_start,
    MAX(date) AS month_end
  FROM FINAL_ASSIGNMENT.GOLD.dim_date
  GROUP BY year, month
),

/* ---------------------------------------------------------
   2) All events occurring inside each calendar month window
      (we will reuse this set for numerator & denominator)
   --------------------------------------------------------- */
events_in_month AS (
  SELECT
    m.year,
    m.month,
    e.CLINICAL_GROUP,
    e.GENDER,
    e.CITY,
    e.STATE,
    e.ORGANIZATION_ID,
    e.PROVIDER_ID,
    e.PATIENT_ID
  FROM months m
  JOIN FINAL_ASSIGNMENT.GOLD.dt_immunization_enriched e
    ON e.IMM_DATE BETWEEN m.month_start AND m.month_end
),

/* -------------------------------------------------------------------
   3) MONTHLY DENOMINATOR (cohort by slice):
      Distinct patients with ANY immunization in that month & slice.
      IMPORTANT: This is NOT grouped by CLINICAL_GROUP on purpose.
   ------------------------------------------------------------------- */
denominator_month AS (
  SELECT
    year,
    month,
    GENDER,
    CITY,
    STATE,
    ORGANIZATION_ID,
    PROVIDER_ID,
    COUNT(DISTINCT PATIENT_ID) AS patients_in_month
  FROM events_in_month
  GROUP BY year, month, GENDER, CITY, STATE, ORGANIZATION_ID, PROVIDER_ID
),

/* ------------------------------------------------------------------------
   4) MONTHLY NUMERATOR (by group & slice):
      Distinct patients vaccinated with the given CLINICAL_GROUP in the month.
   ------------------------------------------------------------------------ */
numerator_month AS (
  SELECT
    year,
    month,
    CLINICAL_GROUP,
    GENDER,
    CITY,
    STATE,
    ORGANIZATION_ID,
    PROVIDER_ID,
    COUNT(DISTINCT PATIENT_ID) AS patients_vaccinated_in_month
  FROM events_in_month
  GROUP BY year, month, CLINICAL_GROUP, GENDER, CITY, STATE, ORGANIZATION_ID, PROVIDER_ID
),

/* ---------------------------------------------------------------
   5) EVER-TO-DATE EVENTS (up to month_end) for cumulative metrics
   --------------------------------------------------------------- */
events_ever AS (
  SELECT
    m.year,
    m.month,
    e.CLINICAL_GROUP,
    e.GENDER,
    e.CITY,
    e.STATE,
    e.ORGANIZATION_ID,
    e.PROVIDER_ID,
    e.PATIENT_ID
  FROM months m
  JOIN FINAL_ASSIGNMENT.GOLD.dt_immunization_enriched e
    ON e.IMM_DATE <= m.month_end
),

/* --------------------------------------------------------------------------
   6) EVER DENOMINATOR (cohort by slice, cumulative):
      Distinct patients with ANY immunization up to month_end in the slice.
      IMPORTANT: Not grouped by CLINICAL_GROUP (cohort is "all immunized").
   -------------------------------------------------------------------------- */
denominator_ever AS (
  SELECT
    year,
    month,
    GENDER,
    CITY,
    STATE,
    ORGANIZATION_ID,
    PROVIDER_ID,
    COUNT(DISTINCT PATIENT_ID) AS patients_ever_to_date
  FROM events_ever
  GROUP BY year, month, GENDER, CITY, STATE, ORGANIZATION_ID, PROVIDER_ID
),

/* -------------------------------------------------------------------------
   7) EVER NUMERATOR (by group & slice, cumulative):
      Distinct patients vaccinated with the given CLINICAL_GROUP up to month_end.
   ------------------------------------------------------------------------- */
numerator_ever AS (
  SELECT
    year,
    month,
    CLINICAL_GROUP,
    GENDER,
    CITY,
    STATE,
    ORGANIZATION_ID,
    PROVIDER_ID,
    COUNT(DISTINCT PATIENT_ID) AS patients_vaccinated_ever_to_date
  FROM events_ever
  GROUP BY year, month, CLINICAL_GROUP, GENDER, CITY, STATE, ORGANIZATION_ID, PROVIDER_ID
),

/* ------------------------------------------------------------
   8) MONTHLY EVENTS COUNT (group & slice):
      Number of immunization events (shots) in the month & slice.
   ------------------------------------------------------------ */
events_count_month AS (
  SELECT
    year,
    month,
    CLINICAL_GROUP,
    GENDER,
    CITY,
    STATE,
    ORGANIZATION_ID,
    PROVIDER_ID,
    COUNT(*) AS events_count
  FROM events_in_month
  GROUP BY year, month, CLINICAL_GROUP, GENDER, CITY, STATE, ORGANIZATION_ID, PROVIDER_ID
)

/* ----------------------------------------
   9) Final KPI rowset: join all components
   ---------------------------------------- */
SELECT
  nm.year,
  nm.month,
  nm.CLINICAL_GROUP,
  nm.GENDER,
  nm.CITY,
  nm.STATE,
  nm.ORGANIZATION_ID,
  nm.PROVIDER_ID,

  /* Month metrics */
  COALESCE(nm.patients_vaccinated_in_month, 0) AS patients_vaccinated_in_month,
  COALESCE(dm.patients_in_month, 0)            AS patients_in_month,
  CASE WHEN dm.patients_in_month > 0
       THEN ROUND(nm.patients_vaccinated_in_month / dm.patients_in_month, 6)
       ELSE 0 END                              AS coverage_pct_month,
  COALESCE(ec.events_count, 0)                 AS events_count,

  /* Ever-to-date metrics */
  COALESCE(ne.patients_vaccinated_ever_to_date, 0) AS patients_vaccinated_ever_to_date,
  COALESCE(de.patients_ever_to_date, 0)            AS patients_ever_to_date,
  CASE WHEN de.patients_ever_to_date > 0
       THEN ROUND(ne.patients_vaccinated_ever_to_date / de.patients_ever_to_date, 6)
       ELSE 0 END                                  AS coverage_pct_ever

FROM numerator_month nm
/* Join MONTH denominator: note NO clinical_group in join (by design) */
LEFT JOIN denominator_month dm
       ON dm.year = nm.year AND dm.month = nm.month
      AND dm.GENDER          = nm.GENDER
      AND dm.CITY            = nm.CITY
      AND dm.STATE           = nm.STATE
      AND dm.ORGANIZATION_ID = nm.ORGANIZATION_ID
      AND dm.PROVIDER_ID     = nm.PROVIDER_ID

/* Join MONTH events count (same full slice + group) */
LEFT JOIN events_count_month ec
       ON ec.year = nm.year AND ec.month = nm.month
      AND ec.CLINICAL_GROUP  = nm.CLINICAL_GROUP
      AND ec.GENDER          = nm.GENDER
      AND ec.CITY            = nm.CITY
      AND ec.STATE           = nm.STATE
      AND ec.ORGANIZATION_ID = nm.ORGANIZATION_ID
      AND ec.PROVIDER_ID     = nm.PROVIDER_ID

/* Join EVER numerator: full slice + group */
LEFT JOIN numerator_ever ne
       ON ne.year = nm.year AND ne.month = nm.month
      AND ne.CLINICAL_GROUP  = nm.CLINICAL_GROUP
      AND ne.GENDER          = nm.GENDER
      AND ne.CITY            = nm.CITY
      AND ne.STATE           = nm.STATE
      AND ne.ORGANIZATION_ID = nm.ORGANIZATION_ID
      AND ne.PROVIDER_ID     = nm.PROVIDER_ID

/* Join EVER denominator: note NO clinical_group in join (by design) */
LEFT JOIN denominator_ever de
       ON de.year = nm.year AND de.month = nm.month
      AND de.GENDER          = nm.GENDER
      AND de.CITY            = nm.CITY
      AND de.STATE           = nm.STATE
      AND de.ORGANIZATION_ID = nm.ORGANIZATION_ID
      AND de.PROVIDER_ID     = nm.PROVIDER_ID
;

select * from FINAL_ASSIGNMENT.GOLD.dt_kpi_immunization_coverage;


-- 1) Coverage should vary; not all rows should be 1.0 anymore
SELECT
  COUNT(*) AS rows_total,
  COUNT_IF(coverage_pct_month < 1) AS rows_lt_1,
  COUNT_IF(coverage_pct_month = 1) AS rows_eq_1
FROM FINAL_ASSIGNMENT.GOLD.dt_kpi_immunization_coverage;

-- 2) Numerator must never exceed denominator (monthly & ever)
SELECT *
FROM FINAL_ASSIGNMENT.GOLD.dt_kpi_immunization_coverage
WHERE patients_vaccinated_in_month > patients_in_month
   OR patients_vaccinated_ever_to_date > patients_ever_to_date;

-- 3) Spot slices with very small denominators (can lead to many 1.0 ratios)
SELECT *
FROM FINAL_ASSIGNMENT.GOLD.dt_kpi_immunization_coverage
WHERE patients_in_month BETWEEN 1 AND 3
ORDER BY year, month;
