-- CREATE DATABASE WEATHER;

-- PARTE 1 ------------------------------------------------------------------------------------------------

CREATE TABLE CLIMA (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    LOCALIDAD VARCHAR(150) NOT NULL,
    PAIS VARCHAR(150) NOT NULL,
    TEMP_CELCIUS DECIMAL(5,2) NOT NULL,
    FECHA_HORA DATETIME2 NOT NULL,
    COVERTURA VARCHAR(50) NOT NULL,
    INDICE_UV DECIMAL(4,2) NOT NULL,
    PRESION_ATM DECIMAL(6,2) NOT NULL,
    VEL_VIENTO_NUDOS DECIMAL(6,2) NOT NULL,
    
    CONSTRAINT LOCALIDAD_FECHA UNIQUE (LOCALIDAD, FECHA_HORA)
);

-- SELECT TOP 5 * FROM CLIMA;

-- PARTE 3 ------------------------------------------------------------------------------------------------

CREATE TABLE CLIMA_DIA (

    ID INT IDENTITY(1,1) PRIMARY KEY,
    LOCALIDAD VARCHAR(150) NOT NULL,
    PAIS VARCHAR(150) NOT NULL,
    AVG_TEMP_FAHRENHEIT DECIMAL(5,2) NOT NULL,
    FECHA DATE NOT NULL, 
    SET_COVERTURA VARCHAR(MAX) NOT NULL,
    AVG_INDICE_UV DECIMAL(4,2) NOT NULL,
    AVG_PRESION_ATM DECIMAL(6,2) NOT NULL,
    AVG_VEL_VIENTO_NUDOS DECIMAL(6,2) NOT NULL,
    
    CONSTRAINT LOCALIDAD_FECHA_DIA UNIQUE (LOCALIDAD, FECHA)
);


-----------------------------


WITH CTE1 AS (
    SELECT
        a.LOCALIDAD                                 AS LOCALIDAD,
        a.PAIS                                      AS PAIS,
        CAST(a.FECHA_HORA AS DATE)                  AS FECHA,
        AVG((a.TEMP_CELCIUS * 9.0/5.0) + 32)        AS AVG_TEMP_FAHRENHEIT,
        STRING_AGG(a.COVERTURA, ', ')               AS SET_COVERTURA,
        AVG(a.INDICE_UV)                            AS AVG_INDICE_UV,
        AVG(a.PRESION_ATM)                          AS AVG_PRESION_ATM,
        AVG(a.VEL_VIENTO_NUDOS)                     AS AVG_VEL_VIENTO_NUDOS
    FROM WEATHER.dbo.CLIMA AS a
    GROUP BY
        a.LOCALIDAD,
        a.PAIS,
        CAST(a.FECHA_HORA AS DATE)
)

INSERT INTO WEATHER.dbo.CLIMA_DIA (
    LOCALIDAD,
    PAIS,
    FECHA,
    AVG_TEMP_FAHRENHEIT,
    SET_COVERTURA,
    AVG_INDICE_UV,
    AVG_PRESION_ATM,
    AVG_VEL_VIENTO_NUDOS
)
SELECT
    LOCALIDAD,
    PAIS,
    FECHA,
    AVG_TEMP_FAHRENHEIT,
    SET_COVERTURA,
    AVG_INDICE_UV,
    AVG_PRESION_ATM,
    AVG_VEL_VIENTO_NUDOS
FROM CTE1;

-- STORED PROCEDURE COMO ADICIONAL --
---------------------------------------------------------
-- USE WEATHER;
-- GO

-- CREATE OR ALTER PROCEDURE dbo.SP_CARGAR_CLIMA_DIA
--     @Fecha DATE   -- Fecha que quieres procesar
-- AS
-- BEGIN
--     SET NOCOUNT ON;

--     DELETE FROM CLIMA_DIA WHERE FECHA = @Fecha

--     ;WITH CTE1 AS (
--         SELECT
--             a.LOCALIDAD                                 AS LOCALIDAD,
--             a.PAIS                                      AS PAIS,
--             CAST(a.FECHA_HORA AS DATE)                  AS FECHA,
--             AVG((a.TEMP_CELCIUS * 9.0/5.0) + 32)        AS AVG_TEMP_FAHRENHEIT,
--             STRING_AGG(a.COVERTURA, ', ')               AS SET_COVERTURA,
--             AVG(a.INDICE_UV)                            AS AVG_INDICE_UV,
--             AVG(a.PRESION_ATM)                          AS AVG_PRESION_ATM,
--             AVG(a.VEL_VIENTO_NUDOS)                     AS AVG_VEL_VIENTO_NUDOS
--         FROM dbo.CLIMA AS a
--         WHERE CAST(a.FECHA_HORA AS DATE) = @Fecha
--         GROUP BY
--             a.LOCALIDAD,
--             a.PAIS,
--             CAST(a.FECHA_HORA AS DATE)
--     )

--     INSERT INTO dbo.CLIMA_DIA (
--         LOCALIDAD,
--         PAIS,
--         FECHA,
--         AVG_TEMP_FAHRENHEIT,
--         SET_COVERTURA,
--         AVG_INDICE_UV,
--         AVG_PRESION_ATM,
--         AVG_VEL_VIENTO_NUDOS
--     )
--     SELECT
--         LOCALIDAD,
--         PAIS,
--         FECHA,
--         AVG_TEMP_FAHRENHEIT,
--         SET_COVERTURA,
--         AVG_INDICE_UV,
--         AVG_PRESION_ATM,
--         AVG_VEL_VIENTO_NUDOS
--     FROM CTE1;
-- END;
-- GO
-----------------------------------------------------------


-- PARTE 4 ------------------------------------------------------------------------------------------------


ALTER TABLE WEATHER.dbo.CLIMA ADD 
    DELTA_TEMP_C DECIMAL(5,2) NULL;

WITH CTE AS (
    SELECT
        ID,
        TEMP_CELCIUS,
        LAG(TEMP_CELCIUS, 1) OVER (
            PARTITION BY LOCALIDAD, PAIS
            ORDER BY FECHA_HORA
        ) AS TEMP_PREVIA
    FROM WEATHER.dbo.CLIMA
)
UPDATE C
SET C.DELTA_TEMP_C = C.TEMP_CELCIUS - T.TEMP_PREVIA
FROM WEATHER.dbo.CLIMA AS C
INNER JOIN CTE AS T
    ON C.ID = T.ID;



ALTER TABLE WEATHER.dbo.CLIMA_DIA ADD 
    DELTA_TEMP_F DECIMAL(5,2) NULL;

WITH CTE AS (
    SELECT
        ID,
        AVG_TEMP_FAHRENHEIT,
        LAG(AVG_TEMP_FAHRENHEIT, 1) OVER (
            PARTITION BY LOCALIDAD, PAIS
            ORDER BY FECHA
        ) AS TEMP_PREVIA
    FROM WEATHER.dbo.CLIMA_DIA
)
UPDATE C
SET C.DELTA_TEMP_F = C.AVG_TEMP_FAHRENHEIT - T.TEMP_PREVIA
FROM WEATHER.dbo.CLIMA_DIA AS C
INNER JOIN CTE AS T
    ON C.ID = T.ID;

--SELECT TOP 100 * FROM WEATHER.dbo.CLIMA_DIA ORDER BY LOCALIDAD;