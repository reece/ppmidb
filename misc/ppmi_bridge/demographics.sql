create or replace view demographics as
select
    rec_id,
    patno, -- as-is
    -- event_id
    -- pag_name
    infodt, -- as-is
,
    = safe_cast_to_boolean (
        code_lookup ('SCREEN', 'AFICBERB', aficberb::text)
    ) as aficberb,
    safe_cast_to_boolean (code_lookup ('SCREEN', 'ASHKJEW', ashkjew::text)) as ashkjew,
    safe_cast_to_boolean (code_lookup ('SCREEN', 'BASQUE', basque::text)) as basque,
    convert_mm_yyyy_to_yyyy_mm (birthdt) as birthdt,
    code_lookup ('SCREEN', 'SEX', sex::text) as sex,
    -- chldbear
    code_lookup ('SCREEN', 'HOWLIVE', howlive::text) as gender,
    -- gayles
    -- hetero
    -- bisexual
    -- pansexual
    -- asexual
    -- othsexuality
    code_lookup ('SCREEN', 'HANDED', handed::text) as handed,
    code_lookup ('SCREEN', 'HISPLAT', hisplat::text) as ethnicity,
    CASE
        WHEN (
            raasian + rablack + rahawopi + raindals + ranos + rawhite + raunknown
        ) = 0 THEN 'Decline to Answer'
        WHEN (
            raasian + rablack + rahawopi + raindals + ranos + rawhite + raunknown
        ) > 1 THEN 'Two or More Races'
        ELSE CASE
            WHEN raasian = 1 THEN code_lookup ('SCREEN', 'RAASIAN', 1::text)
            WHEN rablack = 1 THEN code_lookup ('SCREEN', 'RABLACK', 1::text)
            WHEN rahawopi = 1 THEN code_lookup ('SCREEN', 'RAHAWOPI', 1::text)
            WHEN raindals = 1 THEN code_lookup ('SCREEN', 'RAINDALS', 1::text)
            WHEN ranos = 1 THEN code_lookup ('SCREEN', 'RANOS', 1::text)
            WHEN rawhite = 1 THEN code_lookup ('SCREEN', 'RAWHITE', 1::text)
            WHEN raunknown = 1 THEN code_lookup ('SCREEN', 'RAUNKNOWN', 1::text)
            ELSE NULL
        END
    END AS race,
    convert_mm_yyyy_to_yyyy_mm (orig_entry) as orig_entry,
    last_update,
    (
        select
            max(educyrs::smallint)
        from
            socio_economics SE
        where
            SE.patno = D.patno
    ) as educyrs
from
    demographics D;
