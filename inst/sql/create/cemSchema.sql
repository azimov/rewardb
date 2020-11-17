-- Common evidence model for control mappings
DROP SCHEMA IF EXISTS cem CASCADE;
CREATE SCHEMA cem;

CREATE TABLE cem.matrix_summary (
    ingredient_concept_id INT NOT NULL,
    condition_concept_id INT NOT NULL,
    EVIDENCE_EXISTS INT,
    PRIMARY KEY(ingredient_concept_id, condition_concept_id)
);