MERGE INTO deped.silver.slvr_number_of_public_schools AS target
USING (
    WITH cleaned_data AS (
            SELECT 
                TRIM(
                    REPLACE(
                        SPLIT(region, ' - ')[0], "Region", "")
                ) AS region_code,
                education_level,
                number_of_schools,
                TRIM(
                    REPLACE(school_year, "SY", "")
                ) as school_year,
                source_file, 
                inserted_at
        FROM deped.bronze.brnz_number_of_public_schools
    ),
    deduped AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    region_code,
                    education_level,
                    school_year,
                    number_of_schools
                ORDER BY inserted_at DESC
            ) AS rn
        FROM cleaned_data
    )

    SELECT 
        region_code,
        education_level,
        school_year,
        number_of_schools,
        source_file
    FROM deduped
    WHERE rn = 1
) AS source
ON target.region_code = source.region_code
AND target.education_level = source.region_level
AND target.school_year = source.school_year

WHEN MATCHED THEN
    UPDATE SET
        target.number_of_schools = source.number_of_schools
        target.source_file = source.source_file 

WHEN NOT MATCHED THEN
    INSERT (
        region_code,
        education_level,
        school_year,
        number_of_schools,
        source_file
    ) VALUES (
        source.region_code,
        source.education_level,
        source.school_year,
        source.number_of_schools,
        source.source_file
    )