BEGIN;
CREATE SCHEMA IF NOT EXISTS BL_DM;
CREATE SCHEMA IF NOT EXISTS BL_CL;

DROP TABLE IF EXISTS BL_DM.DIM_CUSTOMERS;
CREATE TABLE IF NOT EXISTS BL_DM.DIM_CUSTOMERS
(
    CUSTOMER_SURR_ID    BIGINT,
    CUSTOMER_SRC_ID     VARCHAR(255)    NOT NULL,
    FIRST_NAME          VARCHAR(50)     NOT NULL,
    LAST_NAME           VARCHAR(50)     NOT NULL,
    EMAIL               VARCHAR(100)    NOT NULL,
    AGE                 SMALLINT        NOT NULL,
    GENDER              VARCHAR(10)     NOT NULL,
    SOURCE_SYSTEM       VARCHAR(255)    NOT NULL,
    SOURCE_ENTITY       VARCHAR(255)    NOT NULL,
    INSERT_DT           TIMESTAMP       NOT NULL,
    UPDATE_DT           TIMESTAMP       NOT NULL,
    CONSTRAINT          PK_DIM_CUSTOMERS_CUSTOMER_SURR_ID PRIMARY KEY (CUSTOMER_SURR_ID),
    CONSTRAINT          UNQ_DIM_CUSTOMERS_CUSTOMER_SRC_ID UNIQUE (CUSTOMER_SRC_ID)
);

DROP SEQUENCE IF EXISTS BL_DM.SEQ_CUSTOMER_SURR_ID;
CREATE SEQUENCE IF NOT EXISTS BL_DM.SEQ_CUSTOMER_SURR_ID START WITH 1 INCREMENT BY 1;

DROP PROCEDURE IF EXISTS BL_CL.SP_DM_LOAD_DIM_CUSTOMERS_DEFAULT;
CREATE OR REPLACE PROCEDURE BL_CL.SP_DM_LOAD_DIM_CUSTOMERS_DEFAULT()
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_AFFECTED BIGINT;
BEGIN
    INSERT INTO BL_DM.DIM_CUSTOMERS (CUSTOMER_SURR_ID, CUSTOMER_SRC_ID, FIRST_NAME, LAST_NAME, EMAIL, AGE, GENDER, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
    VALUES      (-1,
                'N/A',
                'N/A',
                'N/A',
                'N/A',
                -1,
                'N/A',
                'MANUAL',
                'MANUAL',
                '1900-01-01 00:00:00',
                '1900-01-01 00:00:00')
    ON CONFLICT (CUSTOMER_SURR_ID) DO NOTHING;
    
    GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_DM_LOAD_DIM_CUSTOMERS_DEFAULT', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', 1, V_AFFECTED, 0, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_DM_LOAD_DIM_CUSTOMERS_DEFAULT', 'BL_CL', 'ERROR LOADING DIM_CUSTOMERS', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING DIM_CUSTOMERS (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;

DROP TYPE IF EXISTS BL_DM.CUSTOMER_TYPE CASCADE;
CREATE TYPE BL_DM.CUSTOMER_TYPE AS
(
    CUSTOMER_ID     VARCHAR(255),
    FIRST_NAME      VARCHAR(50),
    LAST_NAME       VARCHAR(50),
    EMAIL           VARCHAR(100),
    AGE             SMALLINT,
    GENDER          VARCHAR(10),
    SOURCE_SYSTEM   VARCHAR(255),
    SOURCE_ENTITY   VARCHAR(255)
);

DROP FUNCTION IF EXISTS BL_CL.FN_DM_TRANSFORM_CUSTOMERS;
CREATE OR REPLACE FUNCTION BL_CL.FN_DM_TRANSFORM_CUSTOMERS()
RETURNS SETOF BL_DM.CUSTOMER_TYPE
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_LAST_LOAD_DT TIMESTAMP;
BEGIN
    SELECT  LAST_LOAD_DT
    INTO    V_LAST_LOAD_DT
    FROM    BL_CL.MTA_LOADS
    WHERE   PROCEDURE_NAME = 'SP_DM_LOAD_DIM_CUSTOMERS' AND PROCEDURE_SCHEMA = 'BL_CL';
    
    IF V_LAST_LOAD_DT IS NULL THEN
        V_LAST_LOAD_DT := '1900-01-01 00:00:00';
    END IF;    

    RETURN QUERY
        SELECT  CE.CUSTOMER_ID::VARCHAR(255),
                CE.FIRST_NAME,
                CE.LAST_NAME,
                CE.EMAIL,
                CE.AGE,
                CE.GENDER,
                'BL_3NF'::VARCHAR(255),
                'CE_CUSTOMERS'::VARCHAR(255)
        FROM    BL_3NF.CE_CUSTOMERS AS CE
        WHERE   CE.CUSTOMER_ID > 0 AND
                CE.UPDATE_DT > V_LAST_LOAD_DT;
END;
$$;

DROP PROCEDURE IF EXISTS BL_CL.SP_DM_LOAD_DIM_CUSTOMERS;
CREATE OR REPLACE PROCEDURE BL_CL.SP_DM_LOAD_DIM_CUSTOMERS()
LANGUAGE PLPGSQL
AS $$
DECLARE
    V_CUSTOMER      BL_DM.CUSTOMER_TYPE;
    V_CURSOR        REFCURSOR;
    V_INSERTED_ROWS BIGINT := 0;
    V_UPDATED_ROWS  BIGINT := 0;
    V_TOTAL_ROWS    BIGINT := 0;
    V_AFFECTED      BIGINT := 0;
    V_EXISTS        BOOLEAN;
BEGIN
    OPEN V_CURSOR FOR SELECT * FROM BL_CL.FN_DM_TRANSFORM_CUSTOMERS();

    LOOP
        FETCH V_CURSOR INTO V_CUSTOMER;
        EXIT WHEN NOT FOUND;
    
        SELECT  EXISTS (SELECT 1 FROM BL_DM.DIM_CUSTOMERS WHERE CUSTOMER_SRC_ID = V_CUSTOMER.CUSTOMER_ID)
        INTO    V_EXISTS;
        
        INSERT INTO BL_DM.DIM_CUSTOMERS AS TARGET (CUSTOMER_SURR_ID, CUSTOMER_SRC_ID, FIRST_NAME, LAST_NAME, EMAIL, AGE, GENDER, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
        VALUES      (NEXTVAL('BL_DM.SEQ_CUSTOMER_SURR_ID'),
                    V_CUSTOMER.CUSTOMER_ID, V_CUSTOMER.FIRST_NAME, V_CUSTOMER.LAST_NAME, V_CUSTOMER.EMAIL, V_CUSTOMER.AGE, V_CUSTOMER.GENDER, V_CUSTOMER.SOURCE_SYSTEM, V_CUSTOMER.SOURCE_ENTITY,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP)
        ON CONFLICT (CUSTOMER_SRC_ID) DO UPDATE
        SET         FIRST_NAME = EXCLUDED.FIRST_NAME,
                    LAST_NAME = EXCLUDED.LAST_NAME,
                    EMAIL = EXCLUDED.EMAIL,
                    AGE = EXCLUDED.AGE,
                    GENDER = EXCLUDED.GENDER,
                    UPDATE_DT = CURRENT_TIMESTAMP
        WHERE       TARGET.FIRST_NAME != EXCLUDED.FIRST_NAME OR
                    TARGET.LAST_NAME != EXCLUDED.LAST_NAME OR
                    TARGET.EMAIL != EXCLUDED.EMAIL OR
                    TARGET.AGE != EXCLUDED.AGE OR
                    TARGET.GENDER != EXCLUDED.GENDER;

        GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
        IF V_EXISTS THEN
            V_UPDATED_ROWS := V_UPDATED_ROWS + V_AFFECTED;
        ELSE
            V_INSERTED_ROWS := V_INSERTED_ROWS + V_AFFECTED;
        END IF;
        V_TOTAL_ROWS := V_TOTAL_ROWS + 1;
    END LOOP;
    
    CLOSE V_CURSOR;
    CALL BL_CL.SP_MTA_UPDATE_LOAD('SP_DM_LOAD_DIM_CUSTOMERS', 'BL_CL');
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_DM_LOAD_DIM_CUSTOMERS', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', V_TOTAL_ROWS, V_INSERTED_ROWS, V_UPDATED_ROWS, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    BEGIN
        CLOSE V_CURSOR;
    EXCEPTION WHEN OTHERS THEN END;
    
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_DM_LOAD_DIM_CUSTOMERS', 'BL_CL', 'ERROR LOADING DIM_CUSTOMERS', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING DIM_CUSTOMERS (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;
COMMIT;