CREATE TABLE IF NOT EXISTS deped.gold.gld_number_of_public_schools(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    region_key INT,
    education_level_key INT,
    school_year_key INT,
    government_term_key INT,
    number_of_schools INT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
);
