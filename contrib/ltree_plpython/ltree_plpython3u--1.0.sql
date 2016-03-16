-- make sure the prerequisite libraries are loaded
DO '1' LANGUAGE plpython3u;
SELECT NULL::ltree;


CREATE FUNCTION ltree_to_plpython3(val internal) RETURNS internal
LANGUAGE C STRICT IMMUTABLE
AS 'MODULE_PATHNAME', 'ltree_to_plpython';

CREATE TRANSFORM FOR ltree LANGUAGE plpython3u (
    FROM SQL WITH FUNCTION ltree_to_plpython3(internal)
);
