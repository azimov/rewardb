DROP schema IF EXISTS vocabulary;
CREATE SCHEMA vocabulary;

SET search_path TO vocabulary,public;

/************************

Standardized vocabulary

************************/


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept (
  concept_id			INTEGER			NOT NULL ,
  concept_name			TEXT	NOT NULL ,
  domain_id				VARCHAR(20)		NOT NULL ,
  vocabulary_id			VARCHAR(20)		NOT NULL ,
  concept_class_id		VARCHAR(20)		NOT NULL ,
  standard_concept		VARCHAR(1)		NULL ,
  concept_code			VARCHAR(50)		NOT NULL ,
  valid_start_date		DATE			NOT NULL ,
  valid_end_date		DATE			NOT NULL ,
  invalid_reason		VARCHAR(1)		NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE vocabulary (
  vocabulary_id			VARCHAR(20)		NOT NULL,
  vocabulary_name		TEXT	NOT NULL,
  vocabulary_reference	TEXT	NOT NULL,
  vocabulary_version	TEXT	NULL,
  vocabulary_concept_id	INTEGER			NOT NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE domain (
  domain_id			    VARCHAR(20)		NOT NULL,
  domain_name		    TEXT	NOT NULL,
  domain_concept_id		INTEGER			NOT NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_class (
  concept_class_id			VARCHAR(20)		NOT NULL,
  concept_class_name		TEXT	NOT NULL,
  concept_class_concept_id	INTEGER			NOT NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_relationship (
  concept_id_1			INTEGER			NOT NULL,
  concept_id_2			INTEGER			NOT NULL,
  relationship_id		VARCHAR(20)		NOT NULL,
  valid_start_date		DATE			NOT NULL,
  valid_end_date		DATE			NOT NULL,
  invalid_reason		VARCHAR(1)		NULL
  )
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE relationship (
  relationship_id			VARCHAR(20)		NOT NULL,
  relationship_name			TEXT	NOT NULL,
  is_hierarchical			VARCHAR(1)		NOT NULL,
  defines_ancestry			VARCHAR(1)		NOT NULL,
  reverse_relationship_id	VARCHAR(20)		NOT NULL,
  relationship_concept_id	INTEGER			NOT NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_synonym (
  concept_id			INTEGER			NOT NULL,
  concept_synonym_name	VARCHAR(1000)	NOT NULL,
  language_concept_id	INTEGER			NOT NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE concept_ancestor (
  ancestor_concept_id		INTEGER		NOT NULL,
  descendant_concept_id		INTEGER		NOT NULL,
  min_levels_of_separation	INTEGER		NOT NULL,
  max_levels_of_separation	INTEGER		NOT NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE source_to_concept_map (
  source_code				VARCHAR(50)		NOT NULL,
  source_concept_id			INTEGER			NOT NULL,
  source_vocabulary_id		VARCHAR(20)		NOT NULL,
  source_code_description	TEXT	NULL,
  target_concept_id			INTEGER			NOT NULL,
  target_vocabulary_id		VARCHAR(20)		NOT NULL,
  valid_start_date			DATE			NOT NULL,
  valid_end_date			DATE			NOT NULL,
  invalid_reason			VARCHAR(1)		NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE drug_strength (
  drug_concept_id				INTEGER		  	NOT NULL,
  ingredient_concept_id			INTEGER		  	NOT NULL,
  amount_value					NUMERIC		    NULL,
  amount_unit_concept_id		INTEGER		  	NULL,
  numerator_value				NUMERIC		    NULL,
  numerator_unit_concept_id		INTEGER		  	NULL,
  denominator_value				NUMERIC		    NULL,
  denominator_unit_concept_id	INTEGER		  	NULL,
  box_size						INTEGER		 	NULL,
  valid_start_date				DATE		    NOT NULL,
  valid_end_date				DATE		    NOT NULL,
  invalid_reason				VARCHAR(1)  	NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE attribute_definition (
  attribute_definition_id		  INTEGER			  NOT NULL,
  attribute_name				      TEXT	NOT NULL,
  attribute_description			  TEXT	NULL,
  attribute_type_concept_id		INTEGER			  NOT NULL,
  attribute_syntax				    TEXT	NULL
)
;


/**************************

Standardized meta-data

***************************/


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE cdm_source
(
  cdm_source_name					TEXT	NOT NULL ,
  cdm_source_abbreviation			VARCHAR(25)		NULL ,
  cdm_holder						TEXT	NULL ,
  source_description				TEXT			NULL ,
  source_documentation_reference	TEXT	NULL ,
  cdm_etl_reference					TEXT	NULL ,
  source_release_date				DATE			NULL ,
  cdm_release_date					DATE			NULL ,
  cdm_version						VARCHAR(10)		NULL ,
  vocabulary_version				VARCHAR(20)		NULL
)
;


--HINT DISTRIBUTE ON RANDOM
CREATE TABLE metadata
(
  metadata_concept_id       INTEGER       	NOT NULL ,
  metadata_type_concept_id  INTEGER       	NOT NULL ,
  name                      VARCHAR(250)  	NOT NULL ,
  value_as_string           TEXT  			NULL ,
  value_as_concept_id       INTEGER       	NULL ,
  metadata_date             DATE          	NULL ,
  metadata_datetime         TIMESTAMP      	NULL
)
;