MERGE INTO deped.gold.gld_number_of_public_schools AS target
USING (
    SELECT 
        C.region_key,
        B.education_level_key,
        D.school_year_key,
        E.government_term_key,
        A.number_of_schools
    FROM deped.silver.slvr_number_of_public_schools A
    INNER JOIN deped.dimensions.dim_education_level B
        ON A.education_level = B.education_level_name
    INNER JOIN deped.dimensions.dim_region C
        ON A.region_code = c.region_code
    INNER JOIN deped.dimensions.dim_school_year D
        ON A.school_year = D.school_year
    INNER JOIN deped.dimensions.dim_government_sector E
        ON D.start_year = E.year
) AS source 
ON target.region_key = source.region_key
    AND target.education_level_key = source.education_level_key
    AND target.school_year_key = source.school_year_key
    AND target.government_term_key = source.government_term_key
WHEN MATCHED THEN
    UPDATE SET
        target.number_of_schools = source.number_of_schools
WHEN NOT MATCHED THEN
    INSERT (
        region_key,
        education_level_key,
        school_year_key,
        government_term_key,
        number_of_schools
    )
    VALUES (
        source.region_key,
        source.education_level_key,
        source.school_year_key,
        source.government_term_key,
        source.number_of_schools
    )