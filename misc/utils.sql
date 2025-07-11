CREATE OR REPLACE FUNCTION convert_dscr_to_table_name(dscr text) returns text as $$
select lower(regexp_replace(regexp_replace(dscr, '[- ]', '_', 'g'), '[()]', '', 'g'));
$$
LANGUAGE sql
strict IMMUTABLE;

CREATE OR REPLACE FUNCTION convert_mm_yyyy_to_yyyy_mm(input_date_str TEXT)
RETURNS TEXT AS $$
BEGIN
    IF input_date_str ~ '^\d{2}/\d{4}$' THEN
        RETURN TO_CHAR(TO_DATE(input_date_str, 'MM/YYYY'), 'YYYY-MM');
    ELSE
        RETURN input_date_str;
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION safe_cast_to_boolean(
    p_input_string text
) RETURNS boolean AS $$
DECLARE
    v_result boolean;
BEGIN
    v_result := p_input_string::boolean;
    RETURN v_result;
EXCEPTION
    WHEN invalid_text_representation THEN
        RETURN NULL;
    WHEN others THEN
        RAISE WARNING 'Unexpected error during safe_cast_to_boolean_via_exception for input "%": %', p_input_string, SQLERRM;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

