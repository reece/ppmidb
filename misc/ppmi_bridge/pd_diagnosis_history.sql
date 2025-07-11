CREATE OR REPLACE FUNCTION get_diagnosis_symptom_status (
    p_dxbrady smallint,
    p_dxothsx smallint,
    p_dxposins smallint,
    p_dxrigid smallint,
    p_dxtremor smallint
) RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE
    -- Declare an array to store the names of symptoms with status 'Yes'
    symptom_names TEXT[] := ARRAY[]::TEXT[];
BEGIN
    -- Check each symptom's status.
    -- COALESCE(variable, 0) ensures that NULL values are treated as 0 (No).

    IF COALESCE(p_dxbrady, 0) = 1 THEN
        symptom_names := array_append(symptom_names, 'BRADY');
    END IF;

    IF coalesce(p_dxothsx, 0) = 1 THEN
        symptom_names := array_append(symptom_names, 'OTHSX');
    END IF;

    IF COALESCE(p_dxposins, 0) = 1 THEN
        symptom_names := array_append(symptom_names, 'POSINS');
    END IF;

    IF COALESCE(p_dxrigid, 0) = 1 THEN
        symptom_names := array_append(symptom_names, 'RIGID');
    END IF;

    IF COALESCE(p_dxtremor, 0) = 1 THEN
        symptom_names := array_append(symptom_names, 'TREMOR');
    END IF;

    -- Sort the array of symptom names alphabetically.
    -- We unnest the array, aggregate it back, and order during aggregation.
    SELECT array_agg(x ORDER BY x) INTO symptom_names FROM unnest(symptom_names) AS x;

    -- Convert the sorted array into a comma-separated string and return it.
    RETURN array_to_string(symptom_names, ',');
END;
$$;

create or replace view pd_diagnosis_history as
select
    rec_id,
    patno,
    event_id,
    convert_mm_yyyy_to_yyyy_mm (infodt) as infodt,
    convert_mm_yyyy_to_yyyy_mm (sxdt) as sxdt,
    convert_mm_yyyy_to_yyyy_mm (pddxdt) as pddxdt,
    get_diagnosis_symptom_status (
        p_dxtremor => dxtremor,
        p_dxrigid => dxrigid,
        p_dxbrady => dxbrady,
        p_dxposins => dxposins,
        p_dxothsx => dxothsx
    ) as dx_symptoms,
    code_lookup(pag_name, 'DOMSIDE', domside::text) as domside,
    convert_mm_yyyy_to_yyyy_mm (orig_entry) as orig_entry
from
    pd_diagnosis_history;
