BEGIN;
CREATE SCHEMA IF NOT EXISTS BL_CL;

DROP TABLE IF EXISTS BL_CL.T_MAP_CARD_TYPES;
CREATE TABLE IF NOT EXISTS BL_CL.T_MAP_CARD_TYPES
(
    CARD_TYPE_ID        BIGINT          NOT NULL,
    CARD_TYPE_SRC_ID    VARCHAR(255)    NOT NULL,
    CARD_TYPE_NAME      VARCHAR(50)     NOT NULL,
    VERIFICATION_METHOD VARCHAR(50)     NOT NULL,
    SOURCE_SYSTEM       VARCHAR(255)    NOT NULL,
    SOURCE_ENTITY       VARCHAR(255)    NOT NULL,
    HASH_VALUE          CHAR(32)        NOT NULL,
    LOAD_DT             TIMESTAMP       NOT NULL,
    CONSTRAINT          UNQ_T_MAP_CARD_TYPES_HASH_VALUE UNIQUE (HASH_VALUE)
);

DROP SEQUENCE IF EXISTS BL_CL.SEQ_CARD_TYPE_ID;
CREATE SEQUENCE IF NOT EXISTS BL_CL.SEQ_CARD_TYPE_ID START WITH 1 INCREMENT BY 1;

DROP FUNCTION IF EXISTS BL_CL.FN_CL_TRANSFORM_CARD_TYPES;
CREATE OR REPLACE FUNCTION BL_CL.FN_CL_TRANSFORM_CARD_TYPES()
RETURNS TABLE
(
    CARD_TYPE_NAME      VARCHAR(50),
    VERIFICATION_METHOD VARCHAR(50),
    SOURCE_SYSTEM       VARCHAR(255),
    SOURCE_ENTITY       VARCHAR(255)
)
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_LAST_LOAD_DT TIMESTAMP;
BEGIN
    SELECT  LAST_LOAD_DT
    INTO    V_LAST_LOAD_DT
    FROM    BL_CL.MTA_LOADS
    WHERE   PROCEDURE_NAME = 'SP_CL_LOAD_T_MAP_CARD_TYPES' AND PROCEDURE_SCHEMA = 'BL_CL';

    IF V_LAST_LOAD_DT IS NULL THEN
        V_LAST_LOAD_DT := '1900-01-01 00:00:00';
    END IF;

    RETURN QUERY
        WITH SRC_RANKED_CARD_TYPES AS
        (
            SELECT  UPPER(TRIM(SRC.CARD_TYPE))::VARCHAR                                                                                                                         AS CARD_TYPE_NAME,
                    COALESCE(UPPER(TRIM(SRC.VERIFICATION_METHOD)), 'N/A')::VARCHAR                                                                                              AS VERIFICATION_METHOD,
                    'SA_ONLINE'::VARCHAR                                                                                                                                        AS SOURCE_SYSTEM,
                    'SRC_ONLINE_RETAIL_SALES'::VARCHAR                                                                                                                          AS SOURCE_ENTITY,
                    ROW_NUMBER() OVER (PARTITION BY CONCAT_WS('|', UPPER(TRIM(SRC.CARD_TYPE)), UPPER(TRIM(SRC.VERIFICATION_METHOD))) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC)    AS EVENT_RANK
            FROM    SA_ONLINE.SRC_ONLINE_RETAIL_SALES AS SRC
            WHERE   SRC.CARD_TYPE IS NOT NULL AND
                    SRC.LOAD_DT > V_LAST_LOAD_DT
            UNION ALL
            SELECT  UPPER(TRIM(SRC.CARD_TYPE))::VARCHAR                                                                                                                         AS CARD_TYPE_NAME,
                    COALESCE(UPPER(TRIM(SRC.VERIFICATION_METHOD)), 'N/A')::VARCHAR                                                                                              AS VERIFICATION_METHOD,
                    'SA_OFFLINE'::VARCHAR                                                                                                                                       AS SOURCE_SYSTEM,
                    'SRC_OFFLINE_RETAIL_SALES'::VARCHAR                                                                                                                         AS SOURCE_ENTITY,
                    ROW_NUMBER() OVER (PARTITION BY CONCAT_WS('|', UPPER(TRIM(SRC.CARD_TYPE)), UPPER(TRIM(SRC.VERIFICATION_METHOD))) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC)    AS EVENT_RANK
            FROM    SA_OFFLINE.SRC_OFFLINE_RETAIL_SALES AS SRC
            WHERE   SRC.CARD_TYPE IS NOT NULL AND
                    SRC.LOAD_DT > V_LAST_LOAD_DT
        )
        SELECT  SRC.CARD_TYPE_NAME,
                SRC.VERIFICATION_METHOD,
                SRC.SOURCE_SYSTEM,
                SRC.SOURCE_ENTITY
        FROM    SRC_RANKED_CARD_TYPES AS SRC
        WHERE   SRC.EVENT_RANK = 1;
END;
$$;

DROP PROCEDURE IF EXISTS BL_CL.SP_CL_LOAD_T_MAP_CARD_TYPES;
CREATE OR REPLACE PROCEDURE BL_CL.SP_CL_LOAD_T_MAP_CARD_TYPES()
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_CARD_TYPE_ID  BIGINT;
    V_REC           RECORD;
    V_INSERTED_ROWS BIGINT := 0;
    V_TOTAL_ROWS    BIGINT := 0;
    V_AFFECTED      BIGINT := 0;
BEGIN
    FOR V_REC IN SELECT * FROM BL_CL.FN_CL_TRANSFORM_CARD_TYPES() LOOP
        SELECT  CARD_TYPE_ID
        INTO    V_CARD_TYPE_ID
        FROM    BL_CL.T_MAP_CARD_TYPES
        WHERE   CARD_TYPE_NAME = V_REC.CARD_TYPE_NAME AND
                VERIFICATION_METHOD = V_REC.VERIFICATION_METHOD
        LIMIT   1;

        IF V_CARD_TYPE_ID IS NULL THEN
            V_CARD_TYPE_ID := NEXTVAL('BL_CL.SEQ_CARD_TYPE_ID');
        END IF;
        
        INSERT INTO BL_CL.T_MAP_CARD_TYPES (CARD_TYPE_ID, CARD_TYPE_SRC_ID, CARD_TYPE_NAME, VERIFICATION_METHOD, SOURCE_SYSTEM, SOURCE_ENTITY, HASH_VALUE, LOAD_DT)
        VALUES      (V_CARD_TYPE_ID,
                    CONCAT_WS('|', V_REC.CARD_TYPE_NAME, V_REC.VERIFICATION_METHOD),
                    V_REC.CARD_TYPE_NAME,
                    V_REC.VERIFICATION_METHOD,
                    V_REC.SOURCE_SYSTEM,
                    V_REC.SOURCE_ENTITY,
                    MD5(CONCAT_WS('|', V_REC.CARD_TYPE_NAME, V_REC.VERIFICATION_METHOD, V_REC.SOURCE_SYSTEM, V_REC.SOURCE_ENTITY)),
                    CURRENT_TIMESTAMP)
        ON CONFLICT (HASH_VALUE) DO NOTHING;
        
        GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
        V_INSERTED_ROWS := V_INSERTED_ROWS + V_AFFECTED;
        V_TOTAL_ROWS := V_TOTAL_ROWS + 1;
    END LOOP;
    
    CALL BL_CL.SP_MTA_UPDATE_LOAD('SP_CL_LOAD_T_MAP_CARD_TYPES', 'BL_CL');
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_CL_LOAD_T_MAP_CARD_TYPES', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', V_TOTAL_ROWS, V_INSERTED_ROWS, 0, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_CL_LOAD_T_MAP_CARD_TYPES', 'BL_CL', 'ERROR LOADING T_MAP_CARD_TYPES', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING T_MAP_CARD_TYPES (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;
COMMIT;