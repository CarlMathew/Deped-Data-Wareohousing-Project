MERGE INTO deped.bronze.brnz_number_of_public_schools AS target
USING(
    SELECT DISTINCT
        region,
        education_level,
        CAST(number_of_schools AS INT) AS number_of_schools,
        CAST(school_year AS STRING) AS school_year,
        source_file
    FROM deped.bronze.stg_public_schools       
) AS source

ON target.region = source.region
AND target.education_level = source.education_level
AND target.number_of_schools = source.number_of_schools
AND target.school_year = source.school_year
WHEN NOT MATCHED THEN 
    INSERT(
        region,
        education_level,
        number_of_schools,
        school_year,
        source_file
    ) VALUES (
        source.region,
        source.education_level,
        source.number_of_schools,
        source.school_year,
        source.source_file
    );
