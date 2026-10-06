CREATE TABLE IF NOT EXISTS deped.silver.slvr_number_of_public_schools (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    region_code STRING,
    education_level STRING,
    school_year, INT,
    number_of_schools STRING,
    source_file STRING,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);
