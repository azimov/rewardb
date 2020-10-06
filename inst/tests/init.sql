CREATE SCHEMA vocabulary;

CREATE TABLE vocabulary.concept (
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

COPY vocabulary.concept (
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

CREATE TABLE vocabulary.concept_ancestor (
    ancestor_concept_id real,
    descendant_concept_id real,
    min_levels_of_separation real,
    max_levels_of_separation real
);

COPY vocabulary.concept_ancestor (
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

CREATE TABLE vocabulary.concept_class (
    concept_class_id text,
    concept_class_name text,
    concept_class_concept_id real
);

COPY vocabulary.concept_class (
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

CREATE TABLE vocabulary.concept_relationship (
    concept_id_1 real,
    concept_id_2 real,
    relationship_id text,
    valid_start_date date,
    valid_end_date date,
    invalid_reason text
);

COPY vocabulary.concept_relationship (
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

CREATE TABLE vocabulary.concept_synonym (
    concept_id real,
    concept_synonym_name text,
    language_concept_id real
);

COPY vocabulary.concept_synonym (
    concept_id,
    concept_synonym_name,
    language_concept_id
)
FROM '/eunomia_dt/CONCEPT_SYNONYM.csv'
DELIMITER ','
CSV HEADER;

--
-- TOC entry 236 (class 1259 OID 42013)
-- Name: domain; Type: TABLE; Schema: eunomia; Owner: -
--

CREATE TABLE vocabulary.domain (
    domain_id text,
    domain_name text,
    domain_concept_id real
);

COPY vocabulary.domain (
    domain_id,
    domain_name,
    domain_concept_id
)
FROM '/eunomia_dt/DOMAIN.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE vocabulary.drug_strength (
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

COPY vocabulary.drug_strength (
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


CREATE TABLE vocabulary.relationship (
    relationship_id text,
    relationship_name text,
    is_hierarchical text,
    defines_ancestry text,
    reverse_relationship_id text,
    relationship_concept_id real
);

COPY vocabulary.relationship (
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

CREATE TABLE vocabulary.vocabulary (
    vocabulary_id text,
    vocabulary_name text,
    vocabulary_reference text,
    vocabulary_version text,
    vocabulary_concept_id real
);

COPY vocabulary.vocabulary(vocabulary_id, vocabulary_name, vocabulary_reference, vocabulary_version, vocabulary_concept_id)
FROM '/eunomia_dt/VOCABULARY.csv'
DELIMITER ','
CSV HEADER;