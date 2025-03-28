BEGIN;
CREATE SCHEMA IF NOT EXISTS BL_CL;

DROP TABLE IF EXISTS BL_CL.T_MAP_PRODUCTS;
CREATE TABLE IF NOT EXISTS BL_CL.T_MAP_PRODUCTS 
(
    PRODUCT_ID          BIGINT          NOT NULL,
    PRODUCT_SRC_ID      VARCHAR(255)    NOT NULL,
    PRODUCT_NAME        VARCHAR(100)    NOT NULL,
    MANUFACTURER        VARCHAR(50)     NOT NULL,
    SUBCATEGORY_NAME    VARCHAR(50)     NOT NULL,
    CATEGORY_NAME       VARCHAR(50)     NOT NULL,
    SOURCE_SYSTEM       VARCHAR(255)    NOT NULL,
    SOURCE_ENTITY       VARCHAR(255)    NOT NULL,
    HASH_VALUE          CHAR(32)        NOT NULL,
    LOAD_DT             TIMESTAMP       NOT NULL,
    CONSTRAINT          UNQ_T_MAP_PRODUCTS_HASH_VALUE UNIQUE (HASH_VALUE)
);

DROP SEQUENCE IF EXISTS BL_CL.SEQ_PRODUCT_ID;
CREATE SEQUENCE IF NOT EXISTS BL_CL.SEQ_PRODUCT_ID START WITH 1 INCREMENT BY 1;

DROP FUNCTION IF EXISTS BL_CL.FN_CL_TRANSFORM_PRODUCTS;
CREATE OR REPLACE FUNCTION BL_CL.FN_CL_TRANSFORM_PRODUCTS()
RETURNS TABLE
(
    PRODUCT_ID          VARCHAR(255),
    PRODUCT_NAME        VARCHAR(100),
    MANUFACTURER        VARCHAR(50),
    SUBCATEGORY_NAME    VARCHAR(50),
    CATEGORY_NAME       VARCHAR(50),
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
    WHERE   PROCEDURE_NAME = 'SP_CL_LOAD_T_MAP_PRODUCTS' AND PROCEDURE_SCHEMA = 'BL_CL';

    IF V_LAST_LOAD_DT IS NULL THEN
        V_LAST_LOAD_DT := '1900-01-01 00:00:00';
    END IF;
    
    RETURN QUERY
        WITH SRC_RANKED_PRODUCTS AS
        (
            SELECT  UPPER(TRIM(SRC.PRODUCT_ID))::VARCHAR                                                                AS PRODUCT_ID,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_NAME)), 'N/A')::VARCHAR                                             AS PRODUCT_NAME,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_MANUFACTURER)), 'N/A')::VARCHAR                                     AS MANUFACTURER,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_SUBCATEGORY)), 'N/A')::VARCHAR                                      AS SUBCATEGORY_NAME,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_CATEGORY)), 'N/A')::VARCHAR                                         AS CATEGORY_NAME,
                    'SA_ONLINE'::VARCHAR                                                                                AS SOURCE_SYSTEM,
                    'SRC_ONLINE_RETAIL_SALES'::VARCHAR                                                                  AS SOURCE_ENTITY,
                    ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(SRC.PRODUCT_ID)) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC) AS EVENT_RANK
            FROM    SA_ONLINE.SRC_ONLINE_RETAIL_SALES AS SRC
            WHERE   SRC.PRODUCT_ID IS NOT NULL AND
                    SRC.LOAD_DT > V_LAST_LOAD_DT
            UNION ALL
            SELECT  UPPER(TRIM(SRC.PRODUCT_ID))::VARCHAR                                                                AS PRODUCT_ID,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_NAME)), 'N/A')::VARCHAR                                             AS PRODUCT_NAME,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_MANUFACTURER)), 'N/A')::VARCHAR                                     AS MANUFACTURER,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_SUBCATEGORY)), 'N/A')::VARCHAR                                      AS SUBCATEGORY_NAME,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_CATEGORY)), 'N/A')::VARCHAR                                         AS CATEGORY_NAME,
                    'SA_OFFLINE'::VARCHAR                                                                               AS SOURCE_SYSTEM,
                    'SRC_OFFLINE_RETAIL_SALES'::VARCHAR                                                                 AS SOURCE_ENTITY,
                    ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(SRC.PRODUCT_ID)) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC) AS EVENT_RANK
            FROM    SA_OFFLINE.SRC_OFFLINE_RETAIL_SALES AS SRC
            WHERE   SRC.PRODUCT_ID IS NOT NULL AND
                    SRC.LOAD_DT > V_LAST_LOAD_DT
        )
        SELECT  SRC.PRODUCT_ID,
                SRC.PRODUCT_NAME,
                SRC.MANUFACTURER,
                SRC.SUBCATEGORY_NAME,
                SRC.CATEGORY_NAME,
                SRC.SOURCE_SYSTEM,
                SRC.SOURCE_ENTITY
        FROM    SRC_RANKED_PRODUCTS AS SRC
        WHERE   SRC.EVENT_RANK = 1;
END;
$$;

DROP PROCEDURE IF EXISTS BL_CL.SP_CL_LOAD_T_MAP_PRODUCTS();
CREATE OR REPLACE PROCEDURE BL_CL.SP_CL_LOAD_T_MAP_PRODUCTS()
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_PRODUCT_ID    BIGINT;
    V_REC           RECORD;
    V_INSERTED_ROWS BIGINT := 0;
    V_TOTAL_ROWS    BIGINT := 0;
    V_AFFECTED      BIGINT := 0;
BEGIN
    FOR V_REC IN SELECT * FROM BL_CL.FN_CL_TRANSFORM_PRODUCTS() LOOP
        SELECT  PRODUCT_ID 
        INTO    V_PRODUCT_ID
        FROM    BL_CL.T_MAP_PRODUCTS
        WHERE   PRODUCT_SRC_ID = V_REC.PRODUCT_ID
        LIMIT   1;

        IF V_PRODUCT_ID IS NULL THEN
            V_PRODUCT_ID := NEXTVAL('BL_CL.SEQ_PRODUCT_ID');
        END IF;
        
        INSERT INTO BL_CL.T_MAP_PRODUCTS (PRODUCT_ID, PRODUCT_SRC_ID, PRODUCT_NAME, MANUFACTURER, SUBCATEGORY_NAME, CATEGORY_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, HASH_VALUE, LOAD_DT)
        VALUES      (V_PRODUCT_ID,
                    V_REC.PRODUCT_ID,
                    V_REC.PRODUCT_NAME,
                    V_REC.MANUFACTURER,
                    V_REC.SUBCATEGORY_NAME,
                    V_REC.CATEGORY_NAME,
                    V_REC.SOURCE_SYSTEM,
                    V_REC.SOURCE_ENTITY,
                    MD5(CONCAT_WS('|', V_REC.PRODUCT_ID, V_REC.PRODUCT_NAME, V_REC.MANUFACTURER, V_REC.SUBCATEGORY_NAME, V_REC.CATEGORY_NAME, V_REC.SOURCE_SYSTEM, V_REC.SOURCE_ENTITY)),
                    CURRENT_TIMESTAMP)
        ON CONFLICT (HASH_VALUE) DO NOTHING;
        
        GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
        V_INSERTED_ROWS := V_INSERTED_ROWS + V_AFFECTED;
        V_TOTAL_ROWS := V_TOTAL_ROWS + 1;
    END LOOP;
    
    CALL BL_CL.SP_MTA_UPDATE_LOAD('SP_CL_LOAD_T_MAP_PRODUCTS', 'BL_CL');
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_CL_LOAD_T_MAP_PRODUCTS', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', V_TOTAL_ROWS, V_INSERTED_ROWS, 0, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_CL_LOAD_T_MAP_PRODUCTS', 'BL_CL', 'ERROR LOADING T_MAP_PRODUCTS', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING T_MAP_PRODUCTS (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;
COMMIT;