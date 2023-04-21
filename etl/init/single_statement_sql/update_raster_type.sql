CREATE OR REPLACE FUNCTION raster_send (inp raster) RETURNS bytea AS $$
    DECLARE 
        tmp bytea;
    BEGIN
        tmp := st_ashexwkb(inp);
        RAISE NOTICE 'raster summary: %', st_summary(inp);
        RAISE NOTICE 'sending raster data length: %', length(tmp);
        RAISE NOTICE 'Bytea data: %', tmp;
       RETURN tmp;
    END;
$$ LANGUAGE plpgsql;

--CREATE OR REPLACE FUNCTION raster_recv (outp bytea) RETURNS raster AS $$
--    DECLARE
--        test raster;
--        test2 bytea;
--    BEGIN
--        RAISE NOTICE 'received raster data length: %', length(outp);
--        test2 := substring(outp from 0 for 70);
--        RAISE NOTICE 'substring result: %', test2;
--        --test := st_asraster(st_asewkb(outp));
--        --RAISE NOTICE 'received raster data summary: %', st_summary(test);
--        RETURN st_rastfromwkb(outp);
--    END;
--$$ LANGUAGE plpgsql;

CREATE FUNCTION mytype_recv(internal) RETURNS raster AS $$
    SELECT binary_input_wrapper('raster', 'network', 'raster_input_network', \$1)
$$ LANGUAGE SQL;

CREATE FUNCTION raster_input_network(text, oid) RETURNS mytype AS $$
-- code to read binary value in network byte order and convert it to mytype
$$ LANGUAGE internal;

-- Register a binary output functions for raster data type
UPDATE pg_type SET typsend = 'raster_send', typreceive = 'raster_recv' WHERE oid = (SELECT oid FROM pg_type WHERE typname = 'raster');