BEGIN;
CREATE SCHEMA IF NOT EXISTS BL_3NF;
CREATE SCHEMA IF NOT EXISTS BL_CL;

DROP TABLE IF EXISTS BL_3NF.CE_ADDRESSES;
CREATE TABLE IF NOT EXISTS BL_3NF.CE_ADDRESSES
(
    ADDRESS_ID      BIGINT,
    ADDRESS_SRC_ID  VARCHAR(255)    NOT NULL,
    ADDRESS_NAME    VARCHAR(100)    NOT NULL,
    CITY_ID         BIGINT          NOT NULL,
    SOURCE_SYSTEM   VARCHAR(255)    NOT NULL,
    SOURCE_ENTITY   VARCHAR(255)    NOT NULL,
    INSERT_DT       TIMESTAMP       NOT NULL,
    UPDATE_DT       TIMESTAMP       NOT NULL,
    CONSTRAINT      PK_CE_ADDRESSES_ADDRESS_ID PRIMARY KEY (ADDRESS_ID),
    CONSTRAINT      FK_CE_CITIES_CITY_ID FOREIGN KEY (CITY_ID) REFERENCES BL_3NF.CE_CITIES(CITY_ID),
    CONSTRAINT      UNQ_CE_ADDRESSES_ADDRESS_SRC_ID UNIQUE (ADDRESS_SRC_ID)
);

DROP SEQUENCE IF EXISTS BL_3NF.SEQ_ADDRESS_ID;
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_ADDRESS_ID START WITH 1 INCREMENT BY 1;

DROP PROCEDURE IF EXISTS BL_CL.SP_3NF_LOAD_CE_ADDRESSES_DEFAULT;
CREATE OR REPLACE PROCEDURE BL_CL.SP_3NF_LOAD_CE_ADDRESSES_DEFAULT()
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_AFFECTED BIGINT;
BEGIN
    INSERT INTO BL_3NF.CE_ADDRESSES (ADDRESS_ID, ADDRESS_SRC_ID, ADDRESS_NAME, CITY_ID, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES      (-1,
                'N/A',
                'N/A',
                -1,
                'MANUAL',
                'MANUAL',
                '1900-01-01 00:00:00',
                '1900-01-01 00:00:00')
    ON CONFLICT (ADDRESS_ID) DO NOTHING;
    
    GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_3NF_LOAD_CE_ADDRESSES_DEFAULT', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', 1, V_AFFECTED, 0, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_3NF_LOAD_CE_ADDRESSES_DEFAULT', 'BL_CL', 'ERROR LOADING CE_ADDRESSES', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING CE_ADDRESSES (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;

DROP FUNCTION IF EXISTS BL_CL.FN_3NF_TRANSFORM_ADDRESSES();
CREATE OR REPLACE FUNCTION BL_CL.FN_3NF_TRANSFORM_ADDRESSES()
RETURNS TABLE
(
    ADDRESS_ID      VARCHAR(255),
    ADDRESS_NAME    VARCHAR(100),
    CITY_ID         BIGINT,
    SOURCE_SYSTEM   VARCHAR(255),
    SOURCE_ENTITY   VARCHAR(255)
)
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_LAST_LOAD_DT TIMESTAMP;
BEGIN
    SELECT  LAST_LOAD_DT
    INTO    V_LAST_LOAD_DT
    FROM    BL_CL.MTA_LOADS
    WHERE   PROCEDURE_NAME = 'SP_3NF_LOAD_CE_ADDRESSES' AND PROCEDURE_SCHEMA = 'BL_CL';
    
    IF V_LAST_LOAD_DT IS NULL THEN
        V_LAST_LOAD_DT := '1900-01-01 00:00:00';
    END IF;

    RETURN QUERY
        WITH SRC_ADDRESSES_RANKED AS 
        (
            SELECT  SRC.PURCHASE_ADDRESS,
                    SRC.PURCHASE_CITY,
                    SRC.PURCHASE_COUNTRY,
                    ROW_NUMBER() OVER (PARTITION BY CONCAT_WS('|', UPPER(TRIM(SRC.PURCHASE_ADDRESS)), UPPER(TRIM(SRC.PURCHASE_CITY)), UPPER(TRIM(SRC.PURCHASE_COUNTRY))) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC) AS EVENT_RANK
            FROM    SA_OFFLINE.SRC_OFFLINE_RETAIL_SALES AS SRC
            WHERE   SRC.PURCHASE_ADDRESS IS NOT NULL AND
                    SRC.PURCHASE_CITY IS NOT NULL AND
                    SRC.PURCHASE_COUNTRY IS NOT NULL AND
                    SRC.LOAD_DT > V_LAST_LOAD_DT
        ),
        SRC_ADDRESSES_LATEST AS
        (
            SELECT  SRC.*
            FROM    SRC_ADDRESSES_RANKED AS SRC
            WHERE   SRC.EVENT_RANK = 1
        )
        SELECT          CONCAT_WS('|', UPPER(TRIM(SRC.PURCHASE_ADDRESS)), UPPER(TRIM(SRC.PURCHASE_CITY)), UPPER(TRIM(SRC.PURCHASE_COUNTRY)))::VARCHAR,
                        UPPER(TRIM(SRC.PURCHASE_ADDRESS))::VARCHAR,
                        COALESCE(CE_CIT.CITY_ID, -1),
                        'SA_OFFLINE'::VARCHAR,
                        'SRC_OFFLINE_RETAIL_SALES'::VARCHAR
        FROM            SRC_ADDRESSES_LATEST    AS SRC
        LEFT OUTER JOIN BL_3NF.CE_COUNTRIES     AS CE_COU ON UPPER(TRIM(SRC.PURCHASE_COUNTRY)) = CE_COU.COUNTRY_NAME
        LEFT OUTER JOIN BL_3NF.CE_CITIES        AS CE_CIT ON UPPER(TRIM(SRC.PURCHASE_CITY)) = CE_CIT.CITY_NAME;
END;
$$;

DROP PROCEDURE IF EXISTS BL_CL.SP_3NF_LOAD_CE_ADDRESSES;
CREATE OR REPLACE PROCEDURE BL_CL.SP_3NF_LOAD_CE_ADDRESSES()
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_REC           RECORD;
    V_INSERTED_ROWS BIGINT := 0;
    V_UPDATED_ROWS  BIGINT := 0;
    V_TOTAL_ROWS    BIGINT := 0;
    V_AFFECTED      BIGINT := 0;
    V_EXISTS        BOOLEAN;
BEGIN
    FOR V_REC IN SELECT * FROM BL_CL.FN_3NF_TRANSFORM_ADDRESSES() LOOP
        SELECT  EXISTS (SELECT 1 FROM BL_3NF.CE_ADDRESSES WHERE ADDRESS_SRC_ID = V_REC.ADDRESS_ID)
        INTO    V_EXISTS;
        
        INSERT INTO BL_3NF.CE_ADDRESSES AS TARGET (ADDRESS_ID, ADDRESS_SRC_ID, ADDRESS_NAME, CITY_ID, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
        VALUES      (NEXTVAL('BL_3NF.SEQ_ADDRESS_ID'),
                    V_REC.ADDRESS_ID,
                    V_REC.ADDRESS_NAME, 
                    V_REC.CITY_ID,
                    V_REC.SOURCE_SYSTEM,
                    V_REC.SOURCE_ENTITY,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP)
        ON CONFLICT (ADDRESS_SRC_ID) DO UPDATE
        SET         ADDRESS_NAME = EXCLUDED.ADDRESS_NAME,
                    CITY_ID = EXCLUDED.CITY_ID,
                    UPDATE_DT = CURRENT_TIMESTAMP
        WHERE       TARGET.ADDRESS_NAME != EXCLUDED.ADDRESS_NAME OR
                    TARGET.CITY_ID != EXCLUDED.CITY_ID;
        
        GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
        IF V_EXISTS THEN
            V_UPDATED_ROWS := V_UPDATED_ROWS + V_AFFECTED;
        ELSE
            V_INSERTED_ROWS := V_INSERTED_ROWS + V_AFFECTED;
        END IF;
        V_TOTAL_ROWS := V_TOTAL_ROWS + 1;
    END LOOP;
    
    CALL BL_CL.SP_MTA_UPDATE_LOAD('SP_3NF_LOAD_CE_ADDRESSES', 'BL_CL');
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_3NF_LOAD_CE_ADDRESSES', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', V_TOTAL_ROWS, V_INSERTED_ROWS, V_UPDATED_ROWS, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_3NF_LOAD_CE_ADDRESSES', 'BL_CL', 'ERROR LOADING CE_ADDRESSES', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING CE_ADDRESSES (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;
COMMIT;