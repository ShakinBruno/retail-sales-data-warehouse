BEGIN;
CREATE SCHEMA IF NOT EXISTS BL_CL;

DROP TABLE IF EXISTS BL_CL.T_MAP_PAYMENT_METHODS;
CREATE TABLE IF NOT EXISTS BL_CL.T_MAP_PAYMENT_METHODS
(
    PAYMENT_METHOD_ID       BIGINT          NOT NULL,
    PAYMENT_METHOD_SRC_ID   VARCHAR(255)    NOT NULL,
    PAYMENT_METHOD_NAME     VARCHAR(50)     NOT NULL,
    SOURCE_SYSTEM           VARCHAR(255)    NOT NULL,
    SOURCE_ENTITY           VARCHAR(255)    NOT NULL,
    HASH_VALUE              CHAR(32)        NOT NULL,
    LOAD_DT                 TIMESTAMP       NOT NULL,
    CONSTRAINT              UNQ_T_MAP_PAYMENT_METHODS_HASH_VALUE UNIQUE (HASH_VALUE)
);

DROP SEQUENCE IF EXISTS BL_CL.SEQ_PAYMENT_METHOD_ID;
CREATE SEQUENCE IF NOT EXISTS BL_CL.SEQ_PAYMENT_METHOD_ID START WITH 1 INCREMENT BY 1;

DROP FUNCTION IF EXISTS BL_CL.FN_CL_TRANSFORM_PAYMENT_METHODS;
CREATE OR REPLACE FUNCTION BL_CL.FN_CL_TRANSFORM_PAYMENT_METHODS()
RETURNS TABLE
(
    PAYMENT_METHOD_NAME VARCHAR(50),
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
    WHERE   PROCEDURE_NAME = 'SP_CL_LOAD_T_MAP_PAYMENT_METHODS' AND PROCEDURE_SCHEMA = 'BL_CL';

    IF V_LAST_LOAD_DT IS NULL THEN
        V_LAST_LOAD_DT := '1900-01-01 00:00:00';
    END IF;

    RETURN QUERY
        WITH SRC_RANKED_PAYMENT_METHODS AS
        (
            SELECT  UPPER(TRIM(SRC.PAYMENT_METHOD))::VARCHAR                                                                AS PAYMENT_METHOD_NAME,
                    'SA_ONLINE'::VARCHAR                                                                                    AS SOURCE_SYSTEM,
                    'SRC_ONLINE_RETAIL_SALES'::VARCHAR                                                                      AS SOURCE_ENTITY,
                    ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(SRC.PAYMENT_METHOD)) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC) AS EVENT_RANK
            FROM    SA_ONLINE.SRC_ONLINE_RETAIL_SALES AS SRC
            WHERE   SRC.PAYMENT_METHOD IS NOT NULL AND
                    SRC.LOAD_DT > V_LAST_LOAD_DT
            UNION ALL
            SELECT  UPPER(TRIM(SRC.PAYMENT_METHOD))::VARCHAR                                                                AS PAYMENT_METHOD_NAME,
                    'SA_OFFLINE'::VARCHAR                                                                                   AS SOURCE_SYSTEM,
                    'SRC_OFFLINE_RETAIL_SALES'::VARCHAR                                                                     AS SOURCE_ENTITY,
                    ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(SRC.PAYMENT_METHOD)) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC) AS EVENT_RANK
            FROM    SA_OFFLINE.SRC_OFFLINE_RETAIL_SALES AS SRC
            WHERE   SRC.PAYMENT_METHOD IS NOT NULL AND
                    SRC.LOAD_DT > V_LAST_LOAD_DT
        )
        SELECT  SRC.PAYMENT_METHOD_NAME,
                SRC.SOURCE_SYSTEM,
                SRC.SOURCE_ENTITY
        FROM    SRC_RANKED_PAYMENT_METHODS AS SRC
        WHERE   SRC.EVENT_RANK = 1;
END;
$$;

DROP PROCEDURE IF EXISTS BL_CL.SP_CL_LOAD_T_MAP_PAYMENT_METHODS;
CREATE OR REPLACE PROCEDURE BL_CL.SP_CL_LOAD_T_MAP_PAYMENT_METHODS()
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_PAYMENT_METHOD_ID BIGINT;
    V_REC               RECORD;
    V_INSERTED_ROWS     BIGINT := 0;
    V_TOTAL_ROWS        BIGINT := 0;
    V_AFFECTED          BIGINT := 0;
BEGIN
    FOR V_REC IN SELECT * FROM BL_CL.FN_CL_TRANSFORM_PAYMENT_METHODS() LOOP
        SELECT  PAYMENT_METHOD_ID
        INTO    V_PAYMENT_METHOD_ID
        FROM    BL_CL.T_MAP_PAYMENT_METHODS
        WHERE   PAYMENT_METHOD_NAME = V_REC.PAYMENT_METHOD_NAME
        LIMIT   1;

        IF V_PAYMENT_METHOD_ID IS NULL THEN
            V_PAYMENT_METHOD_ID := NEXTVAL('BL_CL.SEQ_PAYMENT_METHOD_ID');
        END IF;
        
        INSERT INTO BL_CL.T_MAP_PAYMENT_METHODS (PAYMENT_METHOD_ID, PAYMENT_METHOD_SRC_ID, PAYMENT_METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, HASH_VALUE, LOAD_DT)
        VALUES      (V_PAYMENT_METHOD_ID,
                    V_REC.PAYMENT_METHOD_NAME,
                    V_REC.PAYMENT_METHOD_NAME,
                    V_REC.SOURCE_SYSTEM,
                    V_REC.SOURCE_ENTITY,
                    MD5(CONCAT_WS('|', V_REC.PAYMENT_METHOD_NAME, V_REC.SOURCE_SYSTEM, V_REC.SOURCE_ENTITY)),
                    CURRENT_TIMESTAMP)
        ON CONFLICT (HASH_VALUE) DO NOTHING;
        
        GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
        V_INSERTED_ROWS := V_INSERTED_ROWS + V_AFFECTED;
        V_TOTAL_ROWS := V_TOTAL_ROWS + 1;
    END LOOP;
    
    CALL BL_CL.SP_MTA_UPDATE_LOAD('SP_CL_LOAD_T_MAP_PAYMENT_METHODS', 'BL_CL');
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_CL_LOAD_T_MAP_PAYMENT_METHODS', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', V_TOTAL_ROWS, V_INSERTED_ROWS, 0, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_CL_LOAD_T_MAP_PAYMENT_METHODS', 'BL_CL', 'ERROR LOADING T_MAP_PAYMENT_METHODS', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING T_MAP_PAYMENT_METHODS (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;
COMMIT;