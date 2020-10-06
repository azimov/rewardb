--
-- PostgreSQL database dump
--

-- Dumped from database version 12.3
-- Dumped by pg_dump version 12.2

-- Started on 2020-09-09 16:18:50 PDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3 (class 2615 OID 2200)
-- Name: eunomia; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA eunomia;


SET default_table_access_method = heap;

--
-- TOC entry 204 (class 1259 OID 41845)
-- Name: care_site; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.care_site (
    care_site_id real,
    care_site_name text,
    place_of_service_concept_id real,
    location_id real,
    care_site_source_value text,
    place_of_service_source_value text
);

COPY eunomia.care_site (
    care_site_id,
    care_site_name,
    place_of_service_concept_id,
    location_id,
    care_site_source_value,
    place_of_service_source_value
)
FROM '/eunomia_dt/CARE_SITE.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 205 (class 1259 OID 41851)
-- Name: cdm_source; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.cdm_source (
    cdm_source_name text,
    cdm_source_abbreviation text,
    cdm_holder text,
    source_description text,
    source_documentation_reference text,
    cdm_etl_reference text,
    source_release_date date,
    cdm_release_date date,
    cdm_version text,
    vocabulary_version text
);

COPY eunomia.cdm_source (
    cdm_source_name,
    cdm_source_abbreviation,
    cdm_holder,
    source_description,
    source_documentation_reference,
    cdm_etl_reference,
    source_release_date,
    cdm_release_date,
    cdm_version,
    vocabulary_version
)
FROM '/eunomia_dt/CDM_SOURCE.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 206 (class 1259 OID 41857)
-- Name: cohort; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.cohort (
    cohort_definition_id real,
    subject_id real,
    cohort_start_date date,
    cohort_end_date date
);

COPY eunomia.cohort (
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
)
FROM '/eunomia_dt/COHORT.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 207 (class 1259 OID 41860)
-- Name: cohort_attribute; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.cohort_attribute (
    cohort_definition_id real,
    subject_id real,
    cohort_start_date date,
    cohort_end_date date,
    attribute_definition_id real,
    value_as_number real,
    value_as_concept_id real
);


--
-- TOC entry 231 (class 1259 OID 41986)
-- Name: concept; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.concept (
    concept_id real,
    concept_name text,
    domain_id text,
    vocabulary_id text,
    concept_class_id text,
    standard_concept text,
    concept_code text,
    valid_start_date date,
    valid_end_date date,
    invalid_reason text
);

COPY eunomia.concept (
    concept_id,
    concept_name,
    domain_id,
    vocabulary_id,
    concept_class_id,
    standard_concept,
    concept_code,
    valid_start_date,
    valid_end_date,
    invalid_reason
)
FROM '/eunomia_dt/CONCEPT.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 232 (class 1259 OID 41992)
-- Name: concept_ancestor; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.concept_ancestor (
    ancestor_concept_id real,
    descendant_concept_id real,
    min_levels_of_separation real,
    max_levels_of_separation real
);

COPY eunomia.concept_ancestor (
    ancestor_concept_id,
    descendant_concept_id,
    min_levels_of_separation,
    max_levels_of_separation
)
FROM '/eunomia_dt/CONCEPT_ANCESTOR.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 233 (class 1259 OID 41995)
-- Name: concept_class; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.concept_class (
    concept_class_id text,
    concept_class_name text,
    concept_class_concept_id real
);

COPY eunomia.concept_class (
    concept_class_id,
    concept_class_name,
    concept_class_concept_id
)
FROM '/eunomia_dt/CONCEPT_CLASS.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 234 (class 1259 OID 42001)
-- Name: concept_relationship; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.concept_relationship (
    concept_id_1 real,
    concept_id_2 real,
    relationship_id text,
    valid_start_date date,
    valid_end_date date,
    invalid_reason text
);

COPY eunomia.concept_relationship (
    concept_id_1,
    concept_id_2,
    relationship_id,
    valid_start_date,
    valid_end_date,
    invalid_reason
)
FROM '/eunomia_dt/CONCEPT_RELATIONSHIP.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 235 (class 1259 OID 42007)
-- Name: concept_synonym; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.concept_synonym (
    concept_id real,
    concept_synonym_name text,
    language_concept_id real
);

COPY eunomia.concept_synonym (
    concept_id,
    concept_synonym_name,
    language_concept_id
)
FROM '/eunomia_dt/CONCEPT_SYNONYM.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 208 (class 1259 OID 41863)
-- Name: condition_era; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.condition_era (
    condition_era_id real,
    person_id real,
    condition_concept_id real,
    condition_era_start_date date,
    condition_era_end_date date,
    condition_occurrence_count real
);

COPY eunomia.condition_era (
    condition_era_id,
    person_id,
    condition_concept_id,
    condition_era_start_date,
    condition_era_end_date,
    condition_occurrence_count
)
FROM '/eunomia_dt/CONDITION_ERA.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 209 (class 1259 OID 41866)
-- Name: condition_occurrence; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.condition_occurrence (
    condition_occurrence_id real,
    person_id real,
    condition_concept_id real,
    condition_start_date date,
    condition_start_datetime timestamp,
    condition_end_date date,
    condition_end_datetime timestamp,
    condition_type_concept_id real,
    stop_reason text,
    provider_id real,
    visit_occurrence_id real,
    visit_detail_id real,
    condition_source_value text,
    condition_source_concept_id real,
    condition_status_source_value text,
    condition_status_concept_id real
);

COPY eunomia.condition_occurrence (
    condition_occurrence_id,
    person_id,
    condition_concept_id,
    condition_start_date,
    condition_start_datetime,
    condition_end_date,
    condition_end_datetime,
    condition_type_concept_id,
    stop_reason,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    condition_source_value,
    condition_source_concept_id,
    condition_status_source_value,
    condition_status_concept_id
)
FROM '/eunomia_dt/CONDITION_OCCURRENCE.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 210 (class 1259 OID 41872)
-- Name: cost; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.cost (
    cost_id real,
    cost_event_id real,
    cost_domain_id text,
    cost_type_concept_id real,
    currency_concept_id real,
    total_charge real,
    total_cost real,
    total_paid real,
    paid_by_payer real,
    paid_by_patient real,
    paid_patient_copay real,
    paid_patient_coinsurance real,
    paid_patient_deductible real,
    paid_by_primary real,
    paid_ingredient_cost real,
    paid_dispensing_fee real,
    payer_plan_period_id real,
    amount_allowed real,
    revenue_code_concept_id real,
    reveue_code_source_value text,
    drg_concept_id real,
    drg_source_value text
);

COPY eunomia.cost (
    cost_id,
    cost_event_id,
    cost_domain_id,
    cost_type_concept_id,
    currency_concept_id,
    total_charge,
    total_cost,
    total_paid,
    paid_by_payer,
    paid_by_patient,
    paid_patient_copay,
    paid_patient_coinsurance,
    paid_patient_deductible,
    paid_by_primary,
    paid_ingredient_cost,
    paid_dispensing_fee,
    payer_plan_period_id,
    amount_allowed,
    revenue_code_concept_id,
    reveue_code_source_value,
    drg_concept_id,
    drg_source_value
)
FROM '/eunomia_dt/COST.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 211 (class 1259 OID 41878)
-- Name: death; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.death (
    person_id real,
    death_date date,
    death_datetime timestamp,
    death_type_concept_id real,
    cause_concept_id real,
    cause_source_value text,
    cause_source_concept_id real
);

COPY eunomia.death (
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id
)
FROM '/eunomia_dt/DEATH.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 212 (class 1259 OID 41884)
-- Name: device_exposure; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.device_exposure (
    device_exposure_id real,
    person_id real,
    device_concept_id real,
    device_exposure_start_date date,
    device_exposure_start_datetime timestamp,
    device_exposure_end_date date,
    device_exposure_end_datetime timestamp,
    device_type_concept_id real,
    unique_device_id text,
    quantity real,
    provider_id real,
    visit_occurrence_id real,
    visit_detail_id real,
    device_source_value text,
    device_source_concept_id real
);

COPY eunomia.device_exposure (
    device_exposure_id,
    person_id,
    device_concept_id,
    device_exposure_start_date,
    device_exposure_start_datetime,
    device_exposure_end_date,
    device_exposure_end_datetime,
    device_type_concept_id,
    unique_device_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    device_source_value,
    device_source_concept_id
)
FROM '/eunomia_dt/DEVICE_EXPOSURE.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 236 (class 1259 OID 42013)
-- Name: domain; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.domain (
    domain_id text,
    domain_name text,
    domain_concept_id real
);

COPY eunomia.domain (
    domain_id,
    domain_name,
    domain_concept_id
)
FROM '/eunomia_dt/DOMAIN.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 213 (class 1259 OID 41890)
-- Name: dose_era; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.dose_era (
    dose_era_id real,
    person_id real,
    drug_concept_id real,
    unit_concept_id real,
    dose_value real,
    dose_era_start_date date,
    dose_era_end_date date
);

COPY eunomia.dose_era (
    dose_era_id,
    person_id,
    drug_concept_id,
    unit_concept_id,
    dose_value,
    dose_era_start_date,
    dose_era_end_date
)
FROM '/eunomia_dt/DOSE_ERA.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 214 (class 1259 OID 41893)
-- Name: drug_era; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.drug_era (
    drug_era_id real,
    person_id real,
    drug_concept_id real,
    drug_era_start_date date,
    drug_era_end_date date,
    drug_exposure_count real,
    gap_days real
);

COPY eunomia.drug_era (
    drug_era_id,
    person_id,
    drug_concept_id,
    drug_era_start_date,
    drug_era_end_date,
    drug_exposure_count,
    gap_days
)
FROM '/eunomia_dt/DRUG_ERA.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 215 (class 1259 OID 41896)
-- Name: drug_exposure; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.drug_exposure (
    drug_exposure_id real,
    person_id real,
    drug_concept_id real,
    drug_exposure_start_date date,
    drug_exposure_start_datetime timestamp,
    drug_exposure_end_date date,
    drug_exposure_end_datetime timestamp,
    verbatim_end_date date,
    drug_type_concept_id real,
    stop_reason text,
    refills real,
    quantity real,
    days_supply real,
    sig text,
    route_concept_id real,
    lot_number text,
    provider_id real,
    visit_occurrence_id real,
    visit_detail_id real,
    drug_source_value text,
    drug_source_concept_id real,
    route_source_value text,
    dose_unit_source_value text
);

COPY eunomia.drug_exposure (
    drug_exposure_id,
    person_id,
    drug_concept_id,
    drug_exposure_start_date,
    drug_exposure_start_datetime,
    drug_exposure_end_date,
    drug_exposure_end_datetime,
    verbatim_end_date,
    drug_type_concept_id,
    stop_reason,
    refills,
    quantity,
    days_supply,
    sig,
    route_concept_id,
    lot_number,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_value,
    drug_source_concept_id,
    route_source_value,
    dose_unit_source_value
)
FROM '/eunomia_dt/DRUG_EXPOSURE.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 237 (class 1259 OID 42019)
-- Name: drug_strength; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.drug_strength (
    drug_concept_id real,
    ingredient_concept_id real,
    amount_value real,
    amount_unit_concept_id real,
    numerator_value real,
    numerator_unit_concept_id real,
    denominator_value real,
    denominator_unit_concept_id real,
    box_size real,
    valid_start_date date,
    valid_end_date date,
    invalid_reason text
);

COPY eunomia.drug_strength (
    drug_concept_id,
    ingredient_concept_id,
    amount_value,
    amount_unit_concept_id,
    numerator_value,
    numerator_unit_concept_id,
    denominator_value,
    denominator_unit_concept_id,
    box_size,
    valid_start_date,
    valid_end_date,
    invalid_reason
)
FROM '/eunomia_dt/DRUG_STRENGTH.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 216 (class 1259 OID 41902)
-- Name: fact_relationship; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.fact_relationship (
    domain_concept_id_1 real,
    fact_id_1 real,
    domain_concept_id_2 real,
    fact_id_2 real,
    relationship_concept_id real
);

COPY eunomia.fact_relationship (
    domain_concept_id_1,
    fact_id_1,
    domain_concept_id_2,
    fact_id_2,
    relationship_concept_id
)
FROM '/eunomia_dt/FACT_RELATIONSHIP.csv'
DELIMITER ','
CSV HEADER;


--
-- TOC entry 217 (class 1259 OID 41905)
-- Name: location; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.location (
    location_id real,
    address_1 text,
    address_2 text,
    city text,
    state text,
    zip text,
    county text,
    location_source_value text
);

COPY eunomia.location (
    location_id,
    address_1,
    address_2,
    city,
    state,
    zip,
    county,
    location_source_value
)
FROM '/eunomia_dt/LOCATION.csv'
DELIMITER ','
CSV HEADER;


--
-- TOC entry 218 (class 1259 OID 41911)
-- Name: measurement; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.measurement (
    measurement_id real,
    person_id real,
    measurement_concept_id real,
    measurement_date date,
    measurement_datetime timestamp,
    measurement_time text,
    measurement_type_concept_id real,
    operator_concept_id real,
    value_as_number real,
    value_as_concept_id real,
    unit_concept_id real,
    range_low real,
    range_high real,
    provider_id real,
    visit_occurrence_id real,
    visit_detail_id real,
    measurement_source_value text,
    measurement_source_concept_id real,
    unit_source_value text,
    value_source_value text
);

COPY eunomia.measurement (
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_time,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
)
FROM '/eunomia_dt/MEASUREMENT.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 219 (class 1259 OID 41917)
-- Name: metadata; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.metadata (
    metadata_concept_id real,
    metadata_type_concept_id real,
    name text,
    value_as_string text,
    value_as_concept_id real,
    metadata_date date,
    metadata_datetime real
);

COPY eunomia.metadata (
    metadata_concept_id,
    metadata_type_concept_id,
    name,
    value_as_string,
    value_as_concept_id,
    metadata_date,
    metadata_datetime
)
FROM '/eunomia_dt/NOTE.csv'
DELIMITER ','
CSV HEADER;


--
-- TOC entry 220 (class 1259 OID 41923)
-- Name: note; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.note (
    note_id real,
    person_id real,
    note_date date,
    note_datetime timestamp,
    note_type_concept_id real,
    note_class_concept_id real,
    note_title text,
    note_text text,
    encoding_concept_id real,
    language_concept_id real,
    provider_id real,
    visit_occurrence_id real,
    visit_detail_id real,
    note_source_value text
);

COPY eunomia.note (
    note_id,
    person_id,
    note_date,
    note_datetime,
    note_type_concept_id,
    note_class_concept_id,
    note_title,
    note_text,
    encoding_concept_id,
    language_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    note_source_value
)
FROM '/eunomia_dt/NOTE.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 221 (class 1259 OID 41929)
-- Name: note_nlp; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.note_nlp (
    note_nlp_id real,
    note_id real,
    section_concept_id real,
    snippet text,
    "offset" text,
    lexical_variant text,
    note_nlp_concept_id real,
    note_nlp_source_concept_id real,
    nlp_system text,
    nlp_date date,
    nlp_datetime timestamp,
    term_exists text,
    term_temporal text,
    term_modifiers text
);


COPY eunomia.note_nlp (
    note_nlp_id,
    note_id,
    section_concept_id,
    snippet,
    "offset",
    lexical_variant,
    note_nlp_concept_id,
    note_nlp_source_concept_id,
    nlp_system,
    nlp_date,
    nlp_datetime,
    term_exists,
    term_temporal,
    term_modifiers
)
FROM '/eunomia_dt/NOTE_NLP.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 222 (class 1259 OID 41935)
-- Name: observation; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.observation (
    observation_id real,
    person_id real,
    observation_concept_id real,
    observation_date date,
    observation_datetime timestamp,
    observation_type_concept_id real,
    value_as_number real,
    value_as_string text,
    value_as_concept_id real,
    qualifier_concept_id real,
    unit_concept_id real,
    provider_id real,
    visit_occurrence_id real,
    visit_detail_id real,
    observation_source_value text,
    observation_source_concept_id real,
    unit_source_value text,
    qualifier_source_value text
);

COPY eunomia.observation (
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value
)
FROM '/eunomia_dt/OBSERVATION.csv'
DELIMITER ','
CSV HEADER;


--
-- TOC entry 223 (class 1259 OID 41941)
-- Name: observation_period; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.observation_period (
    observation_period_id real,
    person_id real,
    observation_period_start_date date,
    observation_period_end_date date,
    period_type_concept_id real
);

COPY eunomia.observation_period (
    observation_period_id,
    person_id,
    observation_period_start_date,
    observation_period_end_date,
    period_type_concept_id
)
FROM '/eunomia_dt/OBSERVATION_PERIOD.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 224 (class 1259 OID 41944)
-- Name: payer_plan_period; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.payer_plan_period (
    payer_plan_period_id real,
    person_id real,
    payer_plan_period_start_date date,
    payer_plan_period_end_date date,
    payer_concept_id real,
    payer_source_value text,
    payer_source_concept_id real,
    plan_concept_id real,
    plan_source_value text,
    plan_source_concept_id real,
    sponsor_concept_id real,
    sponsor_source_value text,
    sponsor_source_concept_id real,
    family_source_value text,
    stop_reason_concept_id real,
    stop_reason_source_value text,
    stop_reason_source_concept_id real
);


COPY eunomia.payer_plan_period (
    payer_plan_period_id,
    person_id,
    payer_plan_period_start_date,
    payer_plan_period_end_date,
    payer_concept_id,
    payer_source_value,
    payer_source_concept_id,
    plan_concept_id,
    plan_source_value,
    plan_source_concept_id,
    sponsor_concept_id,
    sponsor_source_value,
    sponsor_source_concept_id,
    family_source_value,
    stop_reason_concept_id,
    stop_reason_source_value,
    stop_reason_source_concept_id
)
FROM '/eunomia_dt/PAYER_PLAN_PERIOD.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 225 (class 1259 OID 41950)
-- Name: person; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.person (
    person_id real,
    gender_concept_id real,
    year_of_birth real,
    month_of_birth real,
    day_of_birth real,
    birth_datetime timestamp,
    race_concept_id real,
    ethnicity_concept_id real,
    location_id real,
    provider_id real,
    care_site_id real,
    person_source_value text,
    gender_source_value text,
    gender_source_concept_id real,
    race_source_value text,
    race_source_concept_id real,
    ethnicity_source_value text,
    ethnicity_source_concept_id real
);


COPY eunomia.person (
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id
)
FROM '/eunomia_dt/PERSON.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 226 (class 1259 OID 41956)
-- Name: procedure_occurrence; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.procedure_occurrence (
    procedure_occurrence_id real,
    person_id real,
    procedure_concept_id real,
    procedure_date date,
    procedure_datetime timestamp,
    procedure_type_concept_id real,
    modifier_concept_id real,
    quantity real,
    provider_id real,
    visit_occurrence_id real,
    visit_detail_id real,
    procedure_source_value text,
    procedure_source_concept_id real,
    modifier_source_value text
);

COPY eunomia.procedure_occurrence (
    procedure_occurrence_id,
    person_id,
    procedure_concept_id,
    procedure_date,
    procedure_datetime,
    procedure_type_concept_id,
    modifier_concept_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_value,
    procedure_source_concept_id,
    modifier_source_value
)
FROM '/eunomia_dt/PROCEDURE_OCCURRENCE.csv'
DELIMITER ','
CSV HEADER;
--
-- TOC entry 227 (class 1259 OID 41962)
-- Name: provider; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.provider (
    provider_id real,
    provider_name text,
    npi text,
    dea text,
    specialty_concept_id real,
    care_site_id real,
    year_of_birth real,
    gender_concept_id real,
    provider_source_value text,
    specialty_source_value text,
    specialty_source_concept_id real,
    gender_source_value text,
    gender_source_concept_id real
);

COPY eunomia.provider (
    provider_id ,
    provider_name ,
    npi ,
    dea ,
    specialty_concept_id ,
    care_site_id ,
    year_of_birth ,
    gender_concept_id ,
    provider_source_value ,
    specialty_source_value ,
    specialty_source_concept_id ,
    gender_source_value ,
    gender_source_concept_id
)
FROM '/eunomia_dt/PROVIDER.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 238 (class 1259 OID 42025)
-- Name: relationship; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.relationship (
    relationship_id text,
    relationship_name text,
    is_hierarchical text,
    defines_ancestry text,
    reverse_relationship_id text,
    relationship_concept_id real
);

COPY eunomia.relationship (
    relationship_id,
    relationship_name,
    is_hierarchical,
    defines_ancestry,
    reverse_relationship_id,
    relationship_concept_id
)
FROM '/eunomia_dt/RELATIONSHIP.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 239 (class 1259 OID 42031)
-- Name: source_to_concept_map; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.source_to_concept_map (
    source_code text,
    source_concept_id real,
    source_vocabulary_id text,
    source_code_description text,
    target_concept_id real,
    target_vocabulary_id text,
    valid_start_date date,
    valid_end_date date,
    invalid_reason text
);

COPY eunomia.source_to_concept_map(
    source_code,
    source_concept_id,
    source_vocabulary_id,
    source_code_description,
    target_concept_id,
    target_vocabulary_id,
    valid_start_date,
    valid_end_date,
    invalid_reason
)
FROM '/eunomia_dt/SOURCE_TO_CONCEPT_MAP.csv'
DELIMITER ','
CSV HEADER;


--
-- TOC entry 228 (class 1259 OID 41968)
-- Name: specimen; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.specimen (
    specimen_id real,
    person_id real,
    specimen_concept_id real,
    specimen_type_concept_id real,
    specimen_date date,
    specimen_datetime timestamp,
    quantity real,
    unit_concept_id real,
    anatomic_site_concept_id real,
    disease_status_concept_id real,
    specimen_source_id text,
    specimen_source_value text,
    unit_source_value text,
    anatomic_site_source_value text,
    disease_status_source_value text
);

COPY eunomia.specimen (
    specimen_id,
    person_id,
    specimen_concept_id,
    specimen_type_concept_id,
    specimen_date,
    specimen_datetime,
    quantity,
    unit_concept_id,
    anatomic_site_concept_id,
    disease_status_concept_id,
    specimen_source_id,
    specimen_source_value,
    unit_source_value,
    anatomic_site_source_value,
    disease_status_source_value
)
FROM '/eunomia_dt/SPECIMEN.csv'
DELIMITER ','
CSV HEADER;


--
-- TOC entry 229 (class 1259 OID 41974)
-- Name: visit_detail; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.visit_detail (
    visit_detail_id real,
    person_id real,
    visit_detail_concept_id real,
    visit_detail_start_date date,
    visit_detail_start_datetime timestamp,
    visit_detail_end_date date,
    visit_detail_end_datetime timestamp,
    visit_detail_type_concept_id real,
    provider_id real,
    care_site_id real,
    admitting_source_concept_id real,
    discharge_to_concept_id real,
    preceding_visit_detail_id real,
    visit_detail_source_value text,
    visit_detail_source_concept_id real,
    admitting_source_value text,
    discharge_to_source_value text,
    visit_detail_parent_id real,
    visit_occurrence_id real
);

COPY eunomia.visit_detail (
    visit_detail_id,
    person_id,
    visit_detail_concept_id,
    visit_detail_start_date,
    visit_detail_start_datetime,
    visit_detail_end_date,
    visit_detail_end_datetime,
    visit_detail_type_concept_id,
    provider_id,
    care_site_id,
    admitting_source_concept_id,
    discharge_to_concept_id,
    preceding_visit_detail_id,
    visit_detail_source_value,
    visit_detail_source_concept_id,
    admitting_source_value,
    discharge_to_source_value,
    visit_detail_parent_id,
    visit_occurrence_id
)
FROM '/eunomia_dt/VISIT_DETAIL.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 230 (class 1259 OID 41980)
-- Name: visit_occurrence; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.visit_occurrence (
    visit_occurrence_id real,
    person_id real,
    visit_concept_id real,
    visit_start_date date,
    visit_start_datetime timestamp,
    visit_end_date date,
    visit_end_datetime timestamp,
    visit_type_concept_id real,
    provider_id real,
    care_site_id real,
    visit_source_value text,
    visit_source_concept_id real,
    admitting_source_concept_id real,
    admitting_source_value text,
    discharge_to_concept_id real,
    discharge_to_source_value text,
    preceding_visit_occurrence_id real
);

COPY eunomia.visit_occurrence (visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_datetime, visit_end_date, visit_end_datetime, visit_type_concept_id, provider_id, care_site_id, visit_source_value, visit_source_concept_id, admitting_source_concept_id, admitting_source_value, discharge_to_concept_id, discharge_to_source_value, preceding_visit_occurrence_id)
FROM '/eunomia_dt/VISIT_OCCURRENCE.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 240 (class 1259 OID 42037)
-- Name: vocabulary; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE eunomia.vocabulary (
    vocabulary_id text,
    vocabulary_name text,
    vocabulary_reference text,
    vocabulary_version text,
    vocabulary_concept_id real
);

COPY eunomia.vocabulary(vocabulary_id, vocabulary_name, vocabulary_reference, vocabulary_version, vocabulary_concept_id)
FROM '/eunomia_dt/VOCABULARY.csv'
DELIMITER ','
CSV HEADER;