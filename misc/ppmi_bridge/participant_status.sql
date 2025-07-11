CREATE OR REPLACE FUNCTION get_parkinsons_gene_status(
    p_enrlgba INTEGER,
    p_enrlhpsm INTEGER,
    p_enrllrrk2 INTEGER,
    p_enrlpink1 INTEGER,
    p_enrlprkn INTEGER,
    p_enrlrbd INTEGER,
    p_enrlsnca INTEGER,
    p_enrlsrdc INTEGER
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    -- Declare an array to store the names of genes with enrollment status 'Yes'
    gene_names TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Check each gene's enrollment status.
    -- COALESCE(variable, 0) ensures that NULL values are treated as 0 (No).

    IF COALESCE(p_enrlpink1, 0) = 1 THEN
        gene_names := array_append(gene_names, 'PINK1');
    END IF;

    IF COALESCE(p_enrlprkn, 0) = 1 THEN
        gene_names := array_append(gene_names, 'PRKN');
    END IF;

    IF COALESCE(p_enrlsrdc, 0) = 1 THEN
        -- Assuming 'SRDC' is the intended HGNC gene name based on the column name.
        gene_names := array_append(gene_names, 'SRDC');
    END IF;

    IF COALESCE(p_enrlhpsm, 0) = 1 THEN
        -- Assuming 'HPSM' is the intended HGNC gene name based on the column name.
        gene_names := array_append(gene_names, 'HPSM');
    END IF;

    IF COALESCE(p_enrlrbd, 0) = 1 THEN
        -- Assuming 'RBD' is the intended HGNC gene name based on the column name.
        gene_names := array_append(gene_names, 'RBD');
    END IF;

    IF COALESCE(p_enrllrrk2, 0) = 1 THEN
        gene_names := array_append(gene_names, 'LRRK2');
    END IF;

    IF COALESCE(p_enrlsnca, 0) = 1 THEN
        gene_names := array_append(gene_names, 'SNCA');
    END IF;

    IF COALESCE(p_enrlgba, 0) = 1 THEN
        gene_names := array_append(gene_names, 'GBA');
    END IF;

    -- Sort the array of gene names alphabetically.
    -- We unnest the array, aggregate it back, and order during aggregation.
    SELECT array_agg(x ORDER BY x) INTO gene_names FROM unnest(gene_names) AS x;

    -- Convert the sorted array into a comma-separated string and return it.
    RETURN array_to_string(gene_names, ',');
END;
$$;



create or replace view participant_status as
select
    patno,
    -- cohort,
    cohort_definition,
    convert_mm_yyyy_to_yyyy_mm(enroll_date) as enroll_date,
    enroll_status,
    convert_mm_yyyy_to_yyyy_mm(status_date) as status_date,
    screenedam4,
    enroll_age,
    inexpage,
    av133stdy,
    taustdy,
    gaitstdy,
    pistdy,
    sv2astdy,
    datelig,
    ppmi_online_enroll,
    -- enrlpink1,
    -- enrlprkn,
    -- enrlsrdc,
    -- enrlnorm,
    -- enrlhpsm,
    -- enrlrbd,
    -- enrllrrk2,
    -- enrlsnca,
    -- enrlgba
    get_parkinsons_gene_status(p_enrlgba => enrlgba, p_enrlhpsm => enrlhpsm, p_enrllrrk2 => enrllrrk2, p_enrlpink1 => enrlpink1, p_enrlprkn => enrlprkn, p_enrlrbd => enrlrbd, p_enrlsnca => enrlsnca, p_enrlsrdc => enrlsrdc) as MUTATIONS,
    NULL as subgroup -- from curated data cut

from
    participant_status;
