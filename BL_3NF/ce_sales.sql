BEGIN;
CREATE SCHEMA IF NOT EXISTS BL_3NF;
CREATE SCHEMA IF NOT EXISTS BL_CL;

DROP TABLE IF EXISTS BL_3NF.CE_SALES;
CREATE TABLE IF NOT EXISTS BL_3NF.CE_SALES
(
    SALE_ID             BIGINT,
    SALE_SRC_ID         VARCHAR(255)    NOT NULL,
    EVENT_DT            TIMESTAMP       NOT NULL,
    CUSTOMER_ID         BIGINT          NOT NULL,
    PRODUCT_ID          BIGINT          NOT NULL,
    EMPLOYEE_ID         BIGINT          NOT NULL,
    PAYMENT_DETAIL_ID   BIGINT          NOT NULL,
    DEVICE_ID           BIGINT          NOT NULL,
    QUANTITY            SMALLINT,
    PRICE               DECIMAL(10, 2),
    DISCOUNT            DECIMAL(3, 2),
    TOTAL_AMOUNT        DECIMAL(10, 2),
    SOURCE_SYSTEM       VARCHAR(255)    NOT NULL,
    SOURCE_ENTITY       VARCHAR(255)    NOT NULL,
    INSERT_DT           TIMESTAMP       NOT NULL,
    UPDATE_DT           TIMESTAMP       NOT NULL,
    CONSTRAINT          PK_CE_SALES_SALE_ID PRIMARY KEY (SALE_ID),
    CONSTRAINT          FK_CE_CUSTOMERS_CUSTOMER_ID FOREIGN KEY (CUSTOMER_ID) REFERENCES BL_3NF.CE_CUSTOMERS(CUSTOMER_ID),
    CONSTRAINT          FK_CE_PRODUCTS_PRODUCT_ID FOREIGN KEY (PRODUCT_ID) REFERENCES BL_3NF.CE_PRODUCTS(PRODUCT_ID),
    CONSTRAINT          FK_CE_PAYMENT_DETAILS_PAYMENT_DETAIL_ID FOREIGN KEY (PAYMENT_DETAIL_ID) REFERENCES BL_3NF.CE_PAYMENT_DETAILS(PAYMENT_DETAIL_ID),
    CONSTRAINT          FK_CE_DEVICES_DEVICE_ID FOREIGN KEY (DEVICE_ID) REFERENCES BL_3NF.CE_DEVICES(DEVICE_ID),
    CONSTRAINT          UNQ_CE_SALES_SALE_SRC_ID UNIQUE (SALE_SRC_ID)
);

DROP SEQUENCE IF EXISTS BL_3NF.SEQ_SALE_ID;
CREATE SEQUENCE IF NOT EXISTS BL_3NF.SEQ_SALE_ID START WITH 1 INCREMENT BY 1;

DROP FUNCTION IF EXISTS BL_CL.FN_3NF_TRANSFORM_SALES();
CREATE OR REPLACE FUNCTION BL_CL.FN_3NF_TRANSFORM_SALES()
RETURNS TABLE
(
    SALE_ID             VARCHAR(255),
    EVENT_DT            TIMESTAMP,
    CUSTOMER_ID         BIGINT,
    PRODUCT_ID          BIGINT,
    EMPLOYEE_ID         BIGINT,
    PAYMENT_DETAIL_ID   BIGINT,
    DEVICE_ID           BIGINT,
    QUANTITY            SMALLINT,
    PRICE               DECIMAL(10, 2),
    DISCOUNT            DECIMAL(3, 2),
    TOTAL_AMOUNT        DECIMAL(10, 2),
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
    WHERE   PROCEDURE_NAME = 'SP_3NF_LOAD_CE_SALES' AND PROCEDURE_SCHEMA = 'BL_CL';
    
    IF V_LAST_LOAD_DT IS NULL THEN
        V_LAST_LOAD_DT := '1900-01-01 00:00:00';
    END IF;

    RETURN QUERY
        WITH SRC_SALES_ONLINE_RANKED AS 
        (
            SELECT  SRC.TRANSACTION_ID,
                    SRC.TIMESTAMP,
                    SRC.CUSTOMER_ID,
                    SRC.PRODUCT_ID,
                    SRC.PAYMENT_METHOD,
                    SRC.CARD_TYPE,
                    SRC.VERIFICATION_METHOD,
                    SRC.DEVICE_TYPE,
                    SRC.BROWSER_USED,
                    SRC.PRICE,
                    SRC.QUANTITY,
                    SRC.DISCOUNT,
                    SRC.TOTAL_AMOUNT,
                    ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(SRC.TRANSACTION_ID)) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC) AS EVENT_RANK
            FROM    SA_ONLINE.SRC_ONLINE_RETAIL_SALES AS SRC
            WHERE   SRC.LOAD_DT > V_LAST_LOAD_DT
        ),
        SRC_SALES_OFFLINE_RANKED AS 
        (
            SELECT  SRC.TRANSACTION_ID,
                    SRC.TIMESTAMP,
                    SRC.PRODUCT_ID,
                    SRC.EMPLOYEE_ID,
                    SRC.PAYMENT_METHOD,
                    SRC.CARD_TYPE,
                    SRC.VERIFICATION_METHOD,
                    SRC.PRICE,
                    SRC.QUANTITY,
                    SRC.DISCOUNT,
                    SRC.TOTAL_AMOUNT,
                    ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(SRC.TRANSACTION_ID)) ORDER BY SRC.TIMESTAMP::TIMESTAMP DESC) AS EVENT_RANK
            FROM    SA_OFFLINE.SRC_OFFLINE_RETAIL_SALES AS SRC
            WHERE   SRC.LOAD_DT > V_LAST_LOAD_DT
        ),
        SRC_SALES_TRANSFORMED AS 
        (
            SELECT  UPPER(TRIM(SRC.TRANSACTION_ID))::VARCHAR                        AS SALE_ID,
                    TRIM(SRC.TIMESTAMP)::TIMESTAMP                                  AS EVENT_DT,
                    COALESCE(UPPER(TRIM(SRC.CUSTOMER_ID)), 'N/A')::VARCHAR          AS CUSTOMER_ID,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_ID)), 'N/A')::VARCHAR           AS PRODUCT_ID,
                    'N/A'                                                           AS EMPLOYEE_ID,
                    COALESCE(UPPER(TRIM(SRC.PAYMENT_METHOD)), 'N/A')::VARCHAR       AS PAYMENT_METHOD,
                    COALESCE(UPPER(TRIM(SRC.CARD_TYPE)), 'N/A')::VARCHAR            AS CARD_TYPE,
                    COALESCE(UPPER(TRIM(SRC.VERIFICATION_METHOD)), 'N/A')::VARCHAR  AS VERIFICATION_METHOD,
                    COALESCE(UPPER(TRIM(SRC.DEVICE_TYPE)), 'N/A')::VARCHAR          AS DEVICE_TYPE,
                    COALESCE(UPPER(TRIM(SRC.BROWSER_USED)), 'N/A')::VARCHAR         AS BROWSER_USED,
                    SRC.QUANTITY::SMALLINT                                          AS QUANTITY,
                    SRC.PRICE::DECIMAL                                              AS PRICE,
                    SRC.DISCOUNT::DECIMAL                                           AS DISCOUNT,
                    SRC.TOTAL_AMOUNT::DECIMAL                                       AS TOTAL_AMOUNT,
                    'SA_ONLINE'::VARCHAR                                            AS SOURCE_SYSTEM,
                    'SRC_ONLINE_RETAIL_SALES'::VARCHAR                              AS SOURCE_ENTITY
            FROM    SRC_SALES_ONLINE_RANKED AS SRC
            WHERE   SRC.EVENT_RANK = 1
            UNION ALL
            SELECT  UPPER(TRIM(SRC.TRANSACTION_ID))::VARCHAR                        AS SALE_ID,
                    TRIM(SRC.TIMESTAMP)::TIMESTAMP                                  AS EVENT_DT,
                    'N/A'                                                           AS CUSTOMER_ID,
                    COALESCE(UPPER(TRIM(SRC.PRODUCT_ID)), 'N/A')::VARCHAR           AS PRODUCT_ID,
                    COALESCE(UPPER(TRIM(SRC.EMPLOYEE_ID)), 'N/A')::VARCHAR          AS EMPLOYEE_ID,
                    COALESCE(UPPER(TRIM(SRC.PAYMENT_METHOD)), 'N/A')::VARCHAR       AS PAYMENT_METHOD,
                    COALESCE(UPPER(TRIM(SRC.CARD_TYPE)), 'N/A')::VARCHAR            AS CARD_TYPE,
                    COALESCE(UPPER(TRIM(SRC.VERIFICATION_METHOD)), 'N/A')::VARCHAR  AS VERIFICATION_METHOD,
                    'N/A'                                                           AS DEVICE_TYPE,
                    'N/A'                                                           AS BROWSER_USED,
                    SRC.QUANTITY::SMALLINT                                          AS QUANTITY,
                    SRC.PRICE::DECIMAL                                              AS PRICE,
                    SRC.DISCOUNT::DECIMAL                                           AS DISCOUNT,
                    SRC.TOTAL_AMOUNT::DECIMAL                                       AS TOTAL_AMOUNT,
                    'SA_OFFLINE'::VARCHAR                                           AS SOURCE_SYSTEM,
                    'SRC_OFFLINE_RETAIL_SALES'::VARCHAR                             AS SOURCE_ENTITY
            FROM    SRC_SALES_OFFLINE_RANKED AS SRC
            WHERE   SRC.EVENT_RANK = 1
        )
        SELECT                  SRC.SALE_ID,
                                SRC.EVENT_DT,
                                COALESCE(CE_CUS.CUSTOMER_ID, -1),
                                COALESCE(CE_PRO.PRODUCT_ID, -1),
                                COALESCE(CE_EMP.EMPLOYEE_ID, -1),
                                COALESCE(CE_PAY.PAYMENT_DETAIL_ID, -1),
                                COALESCE(CE_DEV.DEVICE_ID, -1),
                                SRC.QUANTITY,
                                SRC.PRICE,
                                SRC.DISCOUNT,
                                SRC.TOTAL_AMOUNT,
                                SRC.SOURCE_SYSTEM,
                                SRC.SOURCE_ENTITY
        FROM                    SRC_SALES_TRANSFORMED       AS SRC
        LEFT OUTER JOIN LATERAL (SELECT * FROM BL_CL.T_MAP_PRODUCTS WHERE SRC.PRODUCT_ID = PRODUCT_SRC_ID LIMIT 1)                                                                              AS T_PRO ON TRUE
        LEFT OUTER JOIN LATERAL (SELECT * FROM BL_CL.T_MAP_PAYMENT_DETAILS WHERE CONCAT_WS('|', SRC.PAYMENT_METHOD, SRC.CARD_TYPE, SRC.VERIFICATION_METHOD) = PAYMENT_DETAIL_SRC_ID LIMIT 1)    AS T_PAY ON TRUE
        LEFT OUTER JOIN         BL_3NF.CE_CUSTOMERS         AS CE_CUS ON    SRC.CUSTOMER_ID = CE_CUS.CUSTOMER_SRC_ID
        LEFT OUTER JOIN         BL_3NF.CE_PRODUCTS          AS CE_PRO ON    T_PRO.PRODUCT_ID::VARCHAR = CE_PRO.PRODUCT_SRC_ID
        LEFT OUTER JOIN         BL_3NF.CE_EMPLOYEES_SCD     AS CE_EMP ON    SRC.EMPLOYEE_ID = CE_EMP.EMPLOYEE_SRC_ID AND
                                                                            SRC.EVENT_DT BETWEEN CE_EMP.START_DT AND CE_EMP.END_DT
        LEFT OUTER JOIN         BL_3NF.CE_PAYMENT_DETAILS   AS CE_PAY ON    T_PAY.PAYMENT_DETAIL_ID::VARCHAR = CE_PAY.PAYMENT_DETAIL_SRC_ID
        LEFT OUTER JOIN         BL_3NF.CE_DEVICES           AS CE_DEV ON    CONCAT_WS('|', SRC.DEVICE_TYPE, SRC.BROWSER_USED) = CE_DEV.DEVICE_SRC_ID;    
END;
$$;

DROP PROCEDURE IF EXISTS BL_CL.SP_3NF_LOAD_CE_SALES();
CREATE OR REPLACE PROCEDURE BL_CL.SP_3NF_LOAD_CE_SALES()
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
    FOR V_REC IN SELECT * FROM BL_CL.FN_3NF_TRANSFORM_SALES() LOOP
        SELECT  EXISTS (SELECT 1 FROM BL_3NF.CE_SALES WHERE SALE_SRC_ID = V_REC.SALE_ID)
        INTO    V_EXISTS;
        
        INSERT INTO BL_3NF.CE_SALES AS TARGET (SALE_ID, SALE_SRC_ID, EVENT_DT, CUSTOMER_ID, PRODUCT_ID, EMPLOYEE_ID, PAYMENT_DETAIL_ID, DEVICE_ID, QUANTITY, PRICE, DISCOUNT, TOTAL_AMOUNT, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT, UPDATE_DT)
        VALUES      (NEXTVAL('BL_3NF.SEQ_SALE_ID'),
                    V_REC.SALE_ID,
                    V_REC.EVENT_DT,
                    V_REC.CUSTOMER_ID,
                    V_REC.PRODUCT_ID,
                    V_REC.EMPLOYEE_ID,
                    V_REC.PAYMENT_DETAIL_ID,
                    V_REC.DEVICE_ID,
                    V_REC.QUANTITY,
                    V_REC.PRICE,
                    V_REC.DISCOUNT,
                    V_REC.TOTAL_AMOUNT,
                    V_REC.SOURCE_SYSTEM,
                    V_REC.SOURCE_ENTITY,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP)
        ON CONFLICT (SALE_SRC_ID) DO UPDATE
        SET         CUSTOMER_ID = EXCLUDED.CUSTOMER_ID,
                    PRODUCT_ID = EXCLUDED.PRODUCT_ID,
                    EMPLOYEE_ID = EXCLUDED.EMPLOYEE_ID,
                    PAYMENT_DETAIL_ID = EXCLUDED.PAYMENT_DETAIL_ID,
                    DEVICE_ID = EXCLUDED.DEVICE_ID,
                    QUANTITY = EXCLUDED.QUANTITY,
                    PRICE = EXCLUDED.PRICE,
                    DISCOUNT = EXCLUDED.DISCOUNT,
                    TOTAL_AMOUNT = EXCLUDED.TOTAL_AMOUNT
        WHERE       TARGET.CUSTOMER_ID != EXCLUDED.CUSTOMER_ID OR
                    TARGET.PRODUCT_ID != EXCLUDED.PRODUCT_ID OR
                    TARGET.EMPLOYEE_ID != EXCLUDED.EMPLOYEE_ID OR
                    TARGET.PAYMENT_DETAIL_ID != EXCLUDED.PAYMENT_DETAIL_ID OR
                    TARGET.DEVICE_ID != EXCLUDED.DEVICE_ID OR
                    TARGET.QUANTITY != EXCLUDED.QUANTITY OR
                    TARGET.PRICE != EXCLUDED.PRICE OR
                    TARGET.DISCOUNT != EXCLUDED.DISCOUNT OR
                    TARGET.TOTAL_AMOUNT != EXCLUDED.TOTAL_AMOUNT;

        GET DIAGNOSTICS V_AFFECTED = ROW_COUNT;
        IF V_EXISTS THEN
            V_UPDATED_ROWS := V_UPDATED_ROWS + V_AFFECTED;
        ELSE
            V_INSERTED_ROWS := V_INSERTED_ROWS + V_AFFECTED;
        END IF;
        V_TOTAL_ROWS := V_TOTAL_ROWS + 1;
    END LOOP;
    
    CALL BL_CL.SP_MTA_UPDATE_LOAD('SP_3NF_LOAD_CE_SALES', 'BL_CL');
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_3NF_LOAD_CE_SALES', 'BL_CL', 'LOAD PERFORMED SUCCESSFULLY', V_TOTAL_ROWS, V_INSERTED_ROWS, V_UPDATED_ROWS, 'N/A', 'N/A');
EXCEPTION WHEN OTHERS THEN
    CALL BL_CL.SP_MTA_INSERT_LOG('SP_3NF_LOAD_CE_SALES', 'BL_CL', 'ERROR LOADING CE_SALES', -1, -1, -1, UPPER(SQLSTATE), UPPER(SQLERRM));
    COMMIT;
    RAISE EXCEPTION 'ERROR LOADING CE_SALES (%): %', UPPER(SQLSTATE), UPPER(SQLERRM);
END;
$$;
COMMIT;