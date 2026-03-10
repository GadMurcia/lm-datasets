WITH
    dbruto
    AS
    (
        SELECT '|Team|Team id|% Capacity Development|% Capacity Support|% Support No PRD|% Support PRD|\n|Booking Engine|LBE|70%|30%|50%|50%|\n|Customer &amp; Elite Program|ESBIFLY|80%|20%|80%|20%|\n|Financial|LMAF|75%|25%|15%|85%|\n|Integration|LMINT|80%|20%|95%|5%|\n|LM Core|LMCORE|85%|15%|80%|20%|\n|Member Direct &amp; Checkout Page|LMDEV|80%|20%|80%|20%|\n|Mobile App|MOBAPP|80%|20%|80%|20%|\n|Partners &amp; Operations|COMAPP|80%|20%|40%|60%|\n|Payflow &amp; Payment System|LMPAY|70%|30%|75%|25%|\n|Website|WEB|75%|25%|70%|30%' AS description
    ),
    raw_text
    AS
    (
        SELECT SPLIT(description, '\n') AS rows
        FROM dbruto
    ),
    exploded_rows
    AS
    (
        SELECT row_number() OVER () AS row_id,
            TRIM(row) AS row
        FROM raw_text
		CROSS JOIN UNNEST(rows) AS t(row)
    ),
    parsed_table
    AS
    (
        SELECT SPLIT(REGEXP_REPLACE(row, '&amp;', '&'), '|') AS cols
        FROM exploded_rows
        WHERE row_id > 1
    ),
    capacity_teams
    AS
    (
        SELECT TRIM(cols [ 2 ]
    ) AS team,
		TRIM
(cols [ 3 ]) AS team_id,
		CAST
(
			REGEXP_REPLACE
(TRIM
(cols [ 4 ]), '%', '') AS DOUBLE
		) / 100 AS capacity_development,
		CAST
(
			REGEXP_REPLACE
(TRIM
(cols [ 5 ]), '%', '') AS DOUBLE
		) / 100 AS capacity_support,
		CAST
(
			REGEXP_REPLACE
(TRIM
(cols [ 6 ]), '%', '') AS DOUBLE
		) / 100 AS support_no_prd,
		CAST
(
			REGEXP_REPLACE
(TRIM
(cols [ 7 ]), '%', '') AS DOUBLE
		) / 100 AS support_prd
	FROM parsed_table
),
dbruto1 AS
(
    SELECT '|Assigned person|Jira user|LSD resolutor id|Expected monthly sprint swd points|\n|Cristian Salvador Juarez Ramos|Cristian Juárez| |32|\n|Isidro Antonio Flores Martinez|Isidro Antonio Flores Martínez| |32|\n|Jose Ernesto Sanchez Sanchez|Jose Ernesto Sanchez| |32|\n|Melissa Gabriela Melendez Beltran|Melissa Gabriela Meléndez Beltrán| |32|\n|Leonel Armando Cerón Cortez|Leonel Armando Ceron Cortez| |32|' AS description,
        'ESBIFLY' as team_id
UNION ALL
    SELECT '|Assigned person|Jira user|LSD Resolutos ID|Expected monthly sprint swd points|\n|Alejandro Ernesto Mejía Rodríguez|Alejandro Ernesto Mejia Rodriguez| |30|\n|Edwin Ernesto Avilés Rivera|Edwin Ernesto Aviles Rivera| |36|\n|Gabriel Alfonso Rodríguez Solano|Gabriel Alfonso Rodriguez Solano| |20|\n|Jacquelin Guadalupe Angel Caballero|Jacqueline Guadalupe Angel| |20|\n|Javier Alexander Flores de Paz|Javier Flores| |30|\n|Susana Saraí Menjívar Hernández|Susana Saraí Menjívar Hernández| |30|"' AS description,
        'MOBAPP' as team_id
UNION ALL
    SELECT '|Assigned person|Jira User|LSD Resolutor ID|Expected monthly sprint swd points|\n|Denilson Alexander Chávez Recinos|Denilson Alexander Chavez Recinos| |22|\n|Edwin Mauricio Pocasangre Guevara|edwin.pocasangre.SubAVH| |22|\n|Elmer Alfredo Valdez Ramírez|Elmer Alfredo Valdez Ramirez| |22|\n|Javier Aldair Escobar Morales|javier aldair| |22|\n|Leticia Beatriz Olivares Ramos|Leticia Olivares| |22|' AS description,
        'LMAF' as team_id
UNION ALL
    SELECT '|Assigned person|Jira ID|LSD Resolutor ID|Expected monthly sprint swd points|\n|Arnulfo José Aguilar Ramos|Arnulfo Jose Aguilar Ramos| |36|\n|Eduardo Ernesto Flamenco Elías|Eduardo Ernesto Flamenco Elías| |36|\n|Erick Gerardo Sarmiento Valenzuela|Erick Gerado Sarmiento Valenzuela| |36|\n|Henry Alberto Domínguez Vásquez|Henry Alberto Dominguez Vasquez| |36|\n|Javier Armando Ochoa Cerón|Javier Ochoa| |36|\n|Karen Lissette Elías Ramos|Karen Lissette Elias Ramos| |36|\n|Raúl Enrique Orellana Genovés|Raúl Enrique Orellana Genovés| |36|' AS description,
        'LBE' as team_id
UNION ALL
    SELECT '|Assigned person|Jira user|LSD resolutor id|Expected monthly sprint swd points|\n|Andrea María Rodríguez Amaya|Andrea Rodríguez| |30|\n|Christian Gerardo Chinchilla Ramírez|Christian Gerardo Chinchilla Ramirez| |30|\n|Eduardo Antonio Campos Martínez|Eduardo Antonio Campos Martinez| |30|\n|Ever Andrés Jiménez Sánchez|Ever Andres Jimenez Sanchez| |30|\n|Francisco Vásquez Pérez|Francisco Vasquez Perez| |30|\n|Gerson David Ayala Ayala|Gerson David Ayala Ayala| |30|\n|Leonel Armando Cerón Cortez|Leonel Armando Ceron Cortez| |30|\n|Luis Mario González Díaz|Luis Mario Gonzalez| |30|\n|Richard Alexis Avalos García|Richard Alexis Avalos Garcia`| |30|\n|Salvador René Cruz López|Salvador Rene Cruz Lopez| |30|' AS description,
        'LMINT' as team_id
UNION ALL
    SELECT '|Assigned person|Jira user|LSD Resolutos ID|Expected monthly sprint swd points|\n|Diego David Castro Cruz|Diego David Castro Cruz| |30|\n|Hugo Fernando Vélez Osorio|Hugo Fernando Velez Osorio| |30|\n|Mauricio José Mejía Chávez|Mauricio Jose Mejia Chavez| |30|' AS description,
        'LMCORE' as team_id
UNION ALL
    SELECT '|Assigned person|Jira user|LSD Resolutor ID|Expected monthly sprint swd points|\n|Andres Oswaldo Deras Colorado|Andrés Oswaldo Deras Colorado| |24|\n|Bárbara Stefany Aparicio Bermúdez|Barbara Stefany Aparicio Bermudez| |24|\n|Daniel Vladimir Solis Marroquín|Daniel Vladimir Solis Marroquin| |24|\n|Luis Edgardo Castaneda Jovel|Luis Edgardo Castaneda Jovel| |24|\n|Nathaly Michelle Alvarenga González|Nathaly Alvarenga| |24|\n|Wilfredo Emilio Centeno Flores|Wilfredo Emilio Centeno Flores| |24|' AS description,
        'LMDEV' as team_id
UNION ALL
    SELECT '|Assigned person|Jira User|LSD Resolutor ID|Expected monthly sprint swd points|\n|Alexander Alberto Andrade Medina|Alexander Alberto Andrade Medina| |40|\n|Eduardo Vladimir García|Eduardo Vladimir Garcia Tamayo| |40|\n|Nelson Osvaldo Guardado Peraza|Nelson Guardado| |40|' AS description,
        'COMAPP' as team_id
UNION ALL
    SELECT '|Assigned person|Jira user|LSD Resolutor ID|Expected monthly sprint swd points|\n|Cesar Alejandro Rosales|Cesar Alejandro Rosales Cruz| |24|\n|Eliseo Antonio Aguilar Aguilar|Eliseo Antonio Aguilar| |24|\n|Jorge David Hernandez Marroquin|Jorge Hernandez| |24|\n|Luis Eduardo Quintanilla Recinos|luis.quintanilla@lifemiles.com| |24|\n|Tito Erick Carpio Guerra|Tito Erick Carpio Guerra| |24|\n|Edgar Josue Jacinto Rivera|Edgar Josue Jacinto Rivera| |24|' AS description,
        'LMPAY' as team_id
UNION ALL
    SELECT '|Assigned person|Jira user|LSD Resolutor ID|Expected monthly sprint swd points|\n|Ana Saraí Iraheta Rivas|Ana Saraí Iraheta Rivas| |30|\n|Bryan Edenilson Cabrera Cortez|Bryan Edenilson Cabrera Cortez| |30|\n|Carlos Marcelo Cruz Menjívar|Carlos Marcelo Cruz Menjivar| |30|\n|Carlos Rolando Jacinto Rivera|Carlos Rolando Jacinto Rivera| |30|\n|Cecilia Rebeca López Ayala|Cecilia Rebeca Lopez Ayala| |30|\n|Danny Alberto Martínez Dubón|Danny Martinez| |30|\n|Dennis Alexander Tulen Martel|Dennis Alexander Tulen Martel| |30|\n|Edwin Oswaldo Morales Flores|Edwin Morales| |30|\n|Emilio Roberto Deras Urías|Emilio Deras| |30|\n|Gabriela Michelle Alvarez Flores|Gabriela Michelle Alvarez Flores| |30|\n|Guillermo Javier Morales Cornejo|Guillermo Javier Morales Cornejo| |30|\n|Héctor Ramón Rivera González|hrgonzalez1| |30|\n|Herbert David Ayala Bonilla|Herbert David Ayala Bonilla| |30|\n|Iván Nolazco Zepeda|Ivan Nolazco Zepeda| |30|\n|Juan Carlos Menjívar Miranda|Juan Menjivar| |30|\n|Juan Carlos Valencia Nájera|Juan Carlos Valencia Nájera| |30|\n|Kevin Nefeg Escalante Marroquín|Kevin Nefeg Escalante Marroquin| |30|\n|Luis Alfonso Molina Duque|Luis Alfonso Molina Duque| |30|\n|Roberto Antonio Ayala Vasquez|Roberto Ayala| |30|\n|Walter Adolfo Díaz Monge|Walter Adolfo Diaz  Monge| |30|' AS description,
        'WEB' as team_id
)
,
raw_text1 AS
(
	SELECT row_number() OVER () AS id,
    team_id,
    SPLIT(
			REGEXP_REPLACE(description, '&amp;', '&'),
			'\n'
		) AS filas
FROM dbruto1
)
,
exploded_rows1 AS
(
	SELECT id,
    team_id,
    row_number() OVER (PARTITION BY id) AS fila_id,
    TRIM(fila) AS fila
FROM raw_text1
		CROSS JOIN UNNEST(filas) AS t(fila)
)
,
filas_filtradas1 AS
(
	SELECT id,
    team_id,
    SPLIT(fila, '|') AS columnas
FROM exploded_rows1
WHERE fila_id > 1
    AND CARDINALITY(SPLIT(fila, '|')) >= 5
)
,
members_team AS
(
	SELECT team_id,
    TRIM(columnas [ 2 ])
AS assigned_person,
		TRIM
(columnas [ 3 ]) AS jira_user,
		TRIM
(REGEXP_REPLACE
(columnas [ 4 ], '\[', '')) AS lsd_resolutor_id,
		TRIM
(
			CASE
				WHEN columnas [ 4 ] LIKE '%@%' THEN columnas [ 6 ] ELSE columnas [ 5 ]
END
		) AS expected_swd_points
	FROM filas_filtradas1
),
dbruto2 AS
(
--AGOSTO
    SELECT '|Assigned person|Worked days|\n|Cristian Salvador Juarez Ramos|16|\n|Isidro Antonio Flores Martinez|14|\n|Jose Ernesto Sanchez Sanchez|17|\n|Melissa Gabriela Melendez Beltran|18|\n|Leonel Armando Cerón Cortez|17|' as description,
        'ESBIFLY' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Ana Saraí Iraheta Rivas|0|\n|Bryan Edenilson Cabrera Cortez|16|\n|Carlos Marcelo Cruz Menjívar|16|\n|Carlos Rolando Jacinto Rivera|18|\n|Cecilia Rebeca López Ayala|12|\n|Danny Alberto Martínez Dubón|18|\n|Dennis Alexander Tulen Martel|0|\n|Edwin Oswaldo Morales Flores|10|\n|Emilio Roberto Deras Urías|18|\n|Gabriela Michelle Alvarez Flores|13|\n|Guillermo Javier Morales Cornejo|18|\n|Héctor Ramón Rivera González|18|\n|Herbert David Ayala Bonilla|13|\n|Iván Nolazco Zepeda|15|\n|Juan Carlos Menjívar Miranda|15|\n|Juan Carlos Valencia Nájera|15|\n|Kevin Nefeg Escalante Marroquín|15|\n|Luis Alfonso Molina Duque|18|\n|Roberto Antonio Ayala Vasquez|0|\n|Walter Adolfo Díaz Monge|0|' as description,
        'WEB' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Arnulfo José Aguilar Ramos|11|\n|Eduardo Ernesto Flamenco Elías|6|\n|Erick Gerardo Sarmiento Valenzuela|18|\n|Henry Alberto Domínguez Vásquez|16|\n|Javier Armando Ochoa Cerón|18|\n|Karen Lissette Elías Ramos|18|\n|Raúl Enrique Orellana Genovés|16|' as description,
        'LBE' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Denilson Alexander Chávez Recinos|18|\n|Edwin Mauricio Pocasangre Guevara|16|\n|Elmer Alfredo Valdez Ramírez|16|\n|Javier Aldair Escobar Morales|18|\n|Leticia Beatriz Olivares Ramos|18|' as description,
        'LMAF' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andrea María Rodríguez Amaya|16|\n|Christian Gerardo Chinchilla Ramírez|18|\n|Eduardo Antonio Campos Martínez|16|\n|Ever Andrés Jiménez Sánchez|16|\n|Francisco Vásquez Pérez|16|\n|Gerson David Ayala Ayala|16|\n|Leonel Armando Cerón Cortez| |\n|Luis Mario González Díaz|13|\n|Richard Alexis Avalos García|16|\n|Salvador René Cruz López|16|' as description,
        'LMINT' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Diego David Castro Cruz|16|\n|Hugo Fernando Vélez Osorio|16|\n|Mauricio José Mejía Chávez|16|' as description,
        'LMCORE' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andres Oswaldo Deras Colorado|17|\n|Bárbara Stefany Aparicio Bermúdez|13|\n|Daniel Vladimir Solis Marroquín|14|\n|Luis Edgardo Castaneda Jovel|17|\n|Nathaly Michelle Alvarenga González|13|\n|Wilfredo Emilio Centeno Flores|15|' as description,
        'LMDEV' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alejandro Ernesto Mejía Rodríguez|18|\n|Edwin Ernesto Avilés Rivera|15|\n|Gabriel Alfonso Rodríguez Solano|18|\n|Jacquelin Guadalupe Angel Caballero|16|\n|Javier Alexander Flores de Paz|13|\n|Susana Saraí Menjívar Hernández|16|' as description,
        'MOBAPP' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alexander Alberto Andrade Medina|16|\n|Eduardo Vladimir García|16|\n|Nelson Osvaldo Guardado Peraza|15|' as description,
        'COMAPP' as team_id,
        'Work Details 2025-08' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Cesar Alejandro Rosales|16|\n|Eliseo Antonio Aguilar Aguilar|16|\n|Jorge David Hernandez Marroquin|16|\n|Luis Eduardo Quintanilla Recinos|16|\n|Tito Erick Carpio Guerra|0|\n|Edgar Josue Jacinto Rivera|15|' as description,
        'LMPAY' as team_id,
        'Work Details 2025-08' as mes
--FIN AGOSTO
--JULIO
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Cristian Salvador Juarez Ramos|23|\n|Isidro Antonio Flores Martinez|23|\n|Jose Ernesto Sanchez Sanchez|23|\n|Melissa Gabriela Melendez Beltran|14|\n|Leonel Armando Cerón Cortez|23|' as description,
        'ESBIFLY' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Ana Saraí Iraheta Rivas| |\n|Bryan Edenilson Cabrera Cortez|20|\n|Carlos Marcelo Cruz Menjívar|23|\n|Carlos Rolando Jacinto Rivera|23|\n|Cecilia Rebeca López Ayala|20|\n|Danny Alberto Martínez Dubón|23|\n|Dennis Alexander Tulen Martel| |\n|Edwin Oswaldo Morales Flores|22|\n|Emilio Roberto Deras Urías|11|\n|Gabriela Michelle Alvarez Flores|23|\n|Guillermo Javier Morales Cornejo|13|\n|Héctor Ramón Rivera González| |\n|Herbert David Ayala Bonilla|21|\n|Iván Nolazco Zepeda|19|\n|Juan Carlos Menjívar Miranda|23|\n|Juan Carlos Valencia Nájera|23|\n|Kevin Nefeg Escalante Marroquín|20|\n|Luis Alfonso Molina Duque|23|\n|Roberto Antonio Ayala Vasquez| |\n|Walter Adolfo Díaz Monge| |' as description,
        'WEB' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Arnulfo José Aguilar Ramos|23|\n|Eduardo Ernesto Flamenco Elías|23|\n|Erick Gerardo Sarmiento Valenzuela|23|\n|Henry Alberto Domínguez Vásquez|23|\n|Javier Armando Ochoa Cerón|23|\n|Karen Lissette Elías Ramos|23|\n|Raúl Enrique Orellana Genovés|23|' as description,
        'LBE' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Denilson Alexander Chávez Recinos|23|\n|Edwin Mauricio Pocasangre Guevara|23|\n|Elmer Alfredo Valdez Ramírez|23|\n|Javier Aldair Escobar Morales|23|\n|Leticia Beatriz Olivares Ramos|23|' as description,
        'LMAF' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andrea María Rodríguez Amaya|23|\n|Christian Gerardo Chinchilla Ramírez|23|\n|Eduardo Antonio Campos Martínez|23|\n|Ever Andrés Jiménez Sánchez|23|\n|Francisco Vásquez Pérez|23|\n|Gerson David Ayala Ayala|23|\n|Leonel Armando Cerón Cortez| |\n|Luis Mario González Díaz|23|\n|Richard Alexis Avalos García|23|\n|Salvador René Cruz López|23|' as description,
        'LMINT' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Diego David Castro Cruz|23|\n|Hugo Fernando Vélez Osorio|23|\n|Mauricio José Mejía Chávez|23|' as description,
        'LMCORE' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andres Oswaldo Deras Colorado|22|\n|Bárbara Stefany Aparicio Bermúdez|18|\n|Daniel Vladimir Solis Marroquín|23|\n|Luis Edgardo Castaneda Jovel|16|\n|Nathaly Michelle Alvarenga González|23|\n|Wilfredo Emilio Centeno Flores|22|' as description,
        'LMDEV' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alejandro Ernesto Mejía Rodríguez|21|\n|Edwin Ernesto Avilés Rivera|23|\n|Gabriel Alfonso Rodríguez Solano|21|\n|Jacquelin Guadalupe Angel Caballero|23|\n|Javier Alexander Flores de Paz|21|\n|Susana Saraí Menjívar Hernández|22|' as description,
        'MOBAPP' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alexander Alberto Andrade Medina|23|\n|Eduardo Vladimir García|23|\n|Nelson Osvaldo Guardado Peraza|23|' as description,
        'COMAPP' as team_id,
        'Work Details 2025-07' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Cesar Alejandro Rosales|23|\n|Eliseo Antonio Aguilar Aguilar|18|\n|Jorge David Hernandez Marroquin|23|\n|Luis Eduardo Quintanilla Recinos|23|\n|Tito Erick Carpio Guerra|0|\n|Edgar Josue Jacinto Rivera|0|' as description,
        'LMPAY' as team_id,
        'Work Details 2025-07' as mes
--FIN JULIO
--JUNIO
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Cristian Salvador Juarez Ramos|14|\n|Isidro Antonio Flores Martinez|10|\n|Jose Ernesto Sanchez Sanchez|20|\n|Melissa Gabriela Melendez Beltran|21|\n|Leonel Armando Cerón Cortez|21|' as description,
        'ESBIFLY' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Ana Saraí Iraheta Rivas|16|\n|Bryan Edenilson Cabrera Cortez|19|\n|Carlos Marcelo Cruz Menjívar|20|\n|Carlos Rolando Jacinto Rivera|0|\n|Cecilia Rebeca López Ayala|19|\n|Danny Alberto Martínez Dubón|19|\n|Dennis Alexander Tulen Martel|20|\n|Edwin Oswaldo Morales Flores|20|\n|Emilio Roberto Deras Urías|0|\n|Gabriela Michelle Alvarez Flores|20|\n|Guillermo Javier Morales Cornejo|20|\n|Héctor Ramón Rivera González|0|\n|Herbert David Ayala Bonilla|20|\n|Iván Nolazco Zepeda|20|\n|Juan Carlos Menjívar Miranda|20|\n|Juan Carlos Valencia Nájera|17|\n|Kevin Nefeg Escalante Marroquín|19|\n|Luis Alfonso Molina Duque|20|\n|Roberto Antonio Ayala Vasquez|20|\n|Walter Adolfo Díaz Monge|18|' as description,
        'WEB' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Arnulfo José Aguilar Ramos|20|\n|Eduardo Ernesto Flamenco Elías|20|\n|Erick Gerardo Sarmiento Valenzuela|20|\n|Henry Alberto Domínguez Vásquez|20|\n|Javier Armando Ochoa Cerón|12|\n|Karen Lissette Elías Ramos|20|\n|Raúl Enrique Orellana Genovés|20|' as description,
        'LBE' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Denilson Alexander Chávez Recinos|20|\n|Edwin Mauricio Pocasangre Guevara|20|\n|Elmer Alfredo Valdez Ramírez|20|\n|Javier Aldair Escobar Morales|12|\n|Leticia Beatriz Olivares Ramos|20|' as description,
        'LMAF' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andrea María Rodríguez Amaya|20|\n|Christian Gerardo Chinchilla Ramírez|20|\n|Eduardo Antonio Campos Martínez|20|\n|Ever Andrés Jiménez Sánchez|20|\n|Francisco Vásquez Pérez|15|\n|Gerson David Ayala Ayala|20|\n|Leonel Armando Cerón Cortez|20|\n|Luis Mario González Díaz|18|\n|Richard Alexis Avalos García|20|\n|Salvador René Cruz López|16|' as description,
        'LMINT' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Diego David Castro Cruz|20|\n|Hugo Fernando Vélez Osorio|18|\n|Mauricio José Mejía Chávez|19|' as description,
        'LMCORE' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andres Oswaldo Deras Colorado|20|\n|Bárbara Stefany Aparicio Bermúdez|20|\n|Daniel Vladimir Solis Marroquín|20|\n|Luis Edgardo Castaneda Jovel|20|\n|Nathaly Michelle Alvarenga González|20|\n|Wilfredo Emilio Centeno Flores|20|' as description,
        'LMDEV' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alejandro Ernesto Mejía Rodríguez|20|\n|Edwin Ernesto Avilés Rivera|20|\n|Gabriel Alfonso Rodríguez Solano|20|\n|Jacquelin Guadalupe Angel Caballero|20|\n|Javier Alexander Flores de Paz|19|\n|Susana Saraí Menjívar Hernández|18|' as description,
        'MOBAPP' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alexander Alberto Andrade Medina|18|\n|Eduardo Vladimir García|19|\n|Nelson Osvaldo Guardado Peraza|19|' as description,
        'COMAPP' as team_id,
        'Work Details 2025-06' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Cesar Alejandro Rosales|18|\n|Eliseo Antonio Aguilar Aguilar|19|\n|Jorge David Hernandez Marroquin|20|\n|Luis Eduardo Quintanilla Recinos|19|\n|Tito Erick Carpio Guerra|20|\n|Edgar Josue Jacinto Rivera|0|' as description,
        'LMPAY' as team_id,
        'Work Details 2025-06' as mes
--FIN JUNIO
--MAYO
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Cristian Salvador Juarez Ramos|20|\n|Isidro Antonio Flores Martinez|20|\n|Jose Ernesto Sanchez Sanchez|21|\n|Melissa Gabriela Melendez Beltran|22|\n|Leonel Armando Cerón Cortez|22|' as description,
        'ESBIFLY' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Ana Saraí Iraheta Rivas|21|\n|Bryan Edenilson Cabrera Cortez|21|\n|Carlos Marcelo Cruz Menjívar|21|\n|Carlos Rolando Jacinto Rivera|0|\n|Cecilia Rebeca López Ayala|21|\n|Danny Alberto Martínez Dubón|21|\n|Dennis Alexander Tulen Martel|21|\n|Edwin Oswaldo Morales Flores|21|\n|Emilio Roberto Deras Urías|0|\n|Gabriela Michelle Alvarez Flores|21|\n|Guillermo Javier Morales Cornejo|21|\n|Héctor Ramón Rivera González|0|\n|Herbert David Ayala Bonilla|21|\n|Iván Nolazco Zepeda|20|\n|Juan Carlos Menjívar Miranda|16|\n|Juan Carlos Valencia Nájera|21|\n|Kevin Nefeg Escalante Marroquín|21|\n|Luis Alfonso Molina Duque|21|\n|Roberto Antonio Ayala Vasquez|21|\n|Walter Adolfo Díaz Monge|18|' as description,
        'WEB' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Arnulfo José Aguilar Ramos|21|\n|Eduardo Ernesto Flamenco Elías|21|\n|Erick Gerardo Sarmiento Valenzuela|21|\n|Henry Alberto Domínguez Vásquez|14|\n|Javier Armando Ochoa Cerón|20|\n|Karen Lissette Elías Ramos|21|\n|Raúl Enrique Orellana Genovés|13|' as description,
        'LBE' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Denilson Alexander Chávez Recinos|21|\n|Edwin Mauricio Pocasangre Guevara|21|\n|Elmer Alfredo Valdez Ramírez|21|\n|Javier Aldair Escobar Morales|20|\n|Leticia Beatriz Olivares Ramos|21|' as description,
        'LMAF' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andrea María Rodríguez Amaya|21|\n|Christian Gerardo Chinchilla Ramírez|21|\n|Eduardo Antonio Campos Martínez|21|\n|Ever Andrés Jiménez Sánchez|21|\n|Francisco Vásquez Pérez|21|\n|Gerson David Ayala Ayala|21|\n|Leonel Armando Cerón Cortez|21|\n|Luis Mario González Díaz|20|\n|Richard Alexis Avalos García|21|\n|Salvador René Cruz López|21|' as description,
        'LMINT' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Diego David Castro Cruz|18|\n|Hugo Fernando Vélez Osorio|20|\n|Mauricio José Mejía Chávez|20|' as description,
        'LMCORE' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Andres Oswaldo Deras Colorado|21|\n|Bárbara Stefany Aparicio Bermúdez|19|\n|Daniel Vladimir Solis Marroquín|21|\n|Luis Edgardo Castaneda Jovel|21|\n|Nathaly Michelle Alvarenga González|19|\n|Wilfredo Emilio Centeno Flores|14|' as description,
        'LMDEV' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alejandro Ernesto Mejía Rodríguez|21|\n|Edwin Ernesto Avilés Rivera|21|\n|Gabriel Alfonso Rodríguez Solano|20|\n|Jacquelin Guadalupe Angel Caballero|21|\n|Javier Alexander Flores de Paz|21|\n|Susana Saraí Menjívar Hernández|19|' as description,
        'MOBAPP' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Alexander Alberto Andrade Medina|16|\n|Eduardo Vladimir García|16|\n|Nelson Osvaldo Guardado Peraza|15|' as description,
        'COMAPP' as team_id,
        'Work Details 2025-05' as mes
UNION ALL
    SELECT '|Assigned person|Worked days|\n|Cesar Alejandro Rosales|21|\n|Eliseo Antonio Aguilar Aguilar|20|\n|Jorge David Hernandez Marroquin|21|\n|Luis Eduardo Quintanilla Recinos|21|\n|Tito Erick Carpio Guerra|19|\n|Edgar Josue Jacinto Rivera|0|' as description,
        'LMPAY' as team_id,
        'Work Details 2025-05' as mes --FIN MAYO
)
,
raw_text2 AS
(
	SELECT row_number() OVER () AS id,
    team_id,
    SPLIT(mes, ' ') [ 3 ]
as mes,
		SPLIT
(
			REGEXP_REPLACE
(description, '&amp;', '&'),
			'\n'
		) AS filas
	FROM dbruto2
),
exploded_rows2 AS
(
	SELECT id,
    team_id,
    mes,
    row_number() OVER (PARTITION BY id) AS fila_id,
    TRIM(fila) AS fila
FROM raw_text2
		CROSS JOIN UNNEST(filas) AS t(fila)
)
,
filas_filtradas2 AS
(
	SELECT id,
    team_id,
    mes,
    SPLIT(fila, '|') AS columnas
FROM exploded_rows2
WHERE fila_id > 1
    AND CARDINALITY(SPLIT(fila, '|')) >= 2
)
,
worked_days as
(
	SELECT team_id,
    mes,
    TRIM(columnas [ 2 ])
AS assigned_person,
		COALESCE
(TRIM
(columnas [ 3 ]), '0') AS worked_days
	FROM filas_filtradas2
),
sabana AS
(
	SELECT DISTINCT i.key,
    SUBSTRING(i.resolved, 1, 7) AS mes,
    case
			when p.key = 'MOD' then 'WEB' else project_key
		end AS project_key,
    t.name AS tipoIssue,
    CASE
			when customfield_12432 like 'Support' then 'opex' else 'swd'
		END AS capacity,
    CASE
			WHEN customfield_11773 LIKE '%PRD%' THEN 'PRD' ELSE '---'
		END AS source,
    case
			when customfield_11507 IN ('Technology updates', 'Business logic') then 'TU' else '---'
		end AS taskType,
    case
			when p.key = 'MOD' then 'Interactive Technologies' else p.name
		end AS project,
    i.assignee,
    COALESCE(z.issue_report_type, 'no sprint') AS ReportType,
    COALESCE(CAST(customfield_10105 AS DOUBLE), 0) AS points
FROM dev_hudi_rwz_cfn.jira_rwz_issues i
    JOIN dev_hudi_rwz_cfn.jira_rwz_issuetypes t ON t.id = i.issuetype_id
    JOIN dev_hudi_rwz_cfn.jira_rwz_projects p ON p.key = i.project_key
    LEFT JOIN dev_hudi_rwz_cfn.jira_rwz_sprint_issue_report z ON (
			z.key = i.key
        AND z.originboard_id = z.board_id
        AND z.status_name = 'Done'
		)
WHERE i.status = 'Done'
    AND t.name NOT IN (
			'Xray Test',
			'Partial sign-off',
			'Visual testing report',
			'Results',
			'Test Execution',
			'Sub Test Execution',
			'Test Set',
			'Test Plan'
		)
)
,
agruppedData AS
(
	SELECT key,
    mes,
    project_key,
    project,
    assignee,
    ReportType,
    CASE
			WHEN capacity = 'opex' THEN 'swd' ELSE capacity
		END as capacity,
    CASE
			WHEN capacity = 'opex' THEN 'opex'
			WHEN tipoIssue IN ('Configuration') THEN 'opex'
			WHEN tipoIssue = 'Bug'
        AND source = 'PRD' THEN 'opex'
			WHEN tipoIssue = 'Bug'
        AND source = '---' THEN 'capex'
			WHEN tipoIssue = 'Technical Story'
        AND taskType = 'TU' THEN 'capex'
			WHEN tipoIssue = 'Technical Story'
        AND taskType = '---' THEN 'opex'
			WHEN tipoIssue IN (
				'Story',
				'Technical task',
				'Documentation',
				'Story bug',
				'Test Story'
			) THEN 'capex' ELSE CONCAT(tipoIssue, ' ??')
		END AS tipoCargo,
    points
FROM sabana
WHERE points > 0
)
,
sumarizado AS
(
	SELECT mes,
    project_key,
    assignee,
    ReportType,
    capacity,
    tipoCargo,
    SUM(points) AS puntos
FROM agruppedData
GROUP BY mes,
		project_key,
		assignee,
		ReportType,
		capacity,
		tipoCargo
)
,
final AS
(
	SELECT mes,
    project_key,
    assignee,
    SUM(
			CASE
				WHEN ReportType = 'completedIssues' THEN puntos ELSE 0
			END
		) AS puntosCompInSprint,
    SUM(
			CASE
				WHEN ReportType = 'no sprint' THEN puntos ELSE 0
			END
		) AS puntosCompOUTSprint,
    SUM(
			CASE
				WHEN tipoCargo = 'capex'
        AND capacity = 'swd' THEN puntos ELSE 0
			END
		) AS puntosSWDCapex,
    SUM(
			CASE
				WHEN tipoCargo = 'opex'
        AND capacity = 'swd' THEN puntos ELSE 0
			END
		) AS puntosSWDOpex,
    SUM(
			CASE
				WHEN tipoCargo = 'capex'
        AND capacity = 'supp' THEN puntos ELSE 0
			END
		) AS puntosSuppCapex,
    SUM(
			CASE
				WHEN tipoCargo = 'opex'
        AND capacity = 'supp' THEN puntos ELSE 0
			END
		) AS puntosSuppOpex
FROM sumarizado
GROUP BY mes,
		project_key,
		assignee
)
,
swdPoints AS
(
	SELECT DISTINCT mes,
    project_key,
    assignee,
    puntosCompInSprint,
    puntosCompOUTSprint,
    puntosSWDCapex,
    puntosSWDOpex,
    puntosSuppCapex,
    puntosSuppOpex
FROM final
)
,
support_points as
(
	SELECT SUBSTRING(resolved, 1, 7) AS mes,
    --customfield_12204 AS lsd_resolutor_id,
    customfield_12629 AS resolutor,
    CASE
			WHEN customfield_11607 IS NULL THEN 'opex' ELSE 'capex'
		END AS typeOfResource,
    SUM(COALESCE(CAST(customfield_10105 AS DOUBLE), 0)) AS points
FROM "dev_hudi_rwz_cfn"."jira_rwz_not_agile_issues"
WHERE project_key = 'LSD'
--AND customfield_12204 LIKE '%@%'
GROUP BY 1,
		2,
		3
)
,
support_points_by_user AS
(
	SELECT mes,
    resolutor,
    SUM(
			CASE
				WHEN typeOfResource = 'capex' THEN points ELSE 0
			END
		) AS puntos_capex,
    SUM(
			CASE
				WHEN typeOfResource = 'opex' THEN points ELSE 0
			END
		) AS puntos_opex
FROM support_points
GROUP BY mes,
		resolutor
)
SELECT w.mes,
    w.team_id,
    c.team,
    w.assigned_person,
    m.jira_user,
    m.lsd_resolutor_id,
    m.expected_swd_points,
    c.capacity_development,
    c.capacity_support,
    c.support_no_prd,
    c.support_prd,
    w.worked_days,
    dp.puntosCompInSprint,
    dp.puntosCompOUTSprint,
    dp.puntosSWDCapex,
    dp.puntosSWDOpex,
    COALESCE(dp.puntosSuppCapex, 0) + COALESCE(sp.puntos_capex, 0) AS puntosSuppCapex,
    COALESCE(dp.puntosSuppOpex, 0) + COALESCE(sp.puntos_opex, 0) AS puntosSuppOpex
FROM worked_days w
    left join members_team m on (
		m.assigned_person = w.assigned_person
        and w.team_id = m.team_id
	)
    INNER JOIN capacity_teams c on (w.team_id = c.team_id)
    LEFT JOIN swdPoints dp on (
		dp.mes = w.mes
        and dp.project_key = w.team_id
        and dp.assignee = m.jira_user
	)
    LEFT JOIN support_points_by_user sp ON (
		sp.mes = w.mes
        AND sp.resolutor = m.jira_user --m.lsd_resolutor_id
	)