CREATE TABLE IF NOT EXISTS deped.bronze.brnz_number_of_public_teachers (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    region STRING,
    education_level STRING,
    number_of_teachers INT,
    school_year STRING,
    source_file STRING,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);
