WITH
    parametros
    AS
    (
        SELECT <<$inicio>> AS fecha_inicio, 
		<<$fin>> AS fecha_fin
    ),
    fechAS
    AS
    (
        SELECT date_trunc('month', d) AS inicio_mes,
            date_add(
			'second',
			86399,
			CAST(
				date_add(
					'day',
					-1,
					date_add('month', 1, date_trunc('month', d))
				) AS timestamp
			)
		) AS fin_mes
        FROM parametros
		CROSS JOIN UNNEST(
			sequence(
				date_trunc('month', fecha_inicio),
				date_trunc('month', fecha_fin),
				interval
     '1' month
			)
		) AS t
(d)
),
cambios AS
(
	SELECT a.issue_key,
    date_parse(substr(a.created, 1, 19), '%Y-%m-%dT%H:%i:%s') AS fecha,
    a.items_FROMstring,
    a.items_tostring,
    a.items_field
FROM "dev_hudi_rwz_cfn"."jira_rwz_changelog_histories" a
    INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issues" b ON a.issue_key = b.key
    INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" c ON b.issuetype_id = c.id
WHERE c.name = 'Xray Test'
    AND b.project_key = 'TS'
    AND (
			a.items_field LIKE '%labels%'
    OR (
				a.items_field = 'resolution;status'
    AND a.items_FROMstring = 'null;To Do'
    AND a.items_tostring = 'Done;Done'
			)
		)
)
,
cambios2 AS
(
	SELECT issue_key,
    fecha,
    CASE
			WHEN strpos(items_FROMstring, 'manual') > 0 THEN 'Manual'
			WHEN strpos(items_FROMstring, 'pending') > 0 THEN 'Pending'
			WHEN strpos(items_FROMstring, 'Pending') > 0 THEN 'Pending'
			WHEN strpos(items_FROMstring, ' NA ') > 0 THEN 'NA'
			WHEN items_field = 'resolution;status' THEN 'X' ELSE 'Automated'
		END AS anterior,
    CASE
			WHEN strpos(items_tostring, 'manual') > 0 THEN 'Manual'
			WHEN strpos(items_tostring, 'pending') > 0 THEN 'Pending'
			WHEN strpos(items_tostring, 'Pending') > 0 THEN 'Pending'
			WHEN strpos(items_tostring, ' NA ') > 0 THEN 'NA'
			WHEN items_field = 'resolution;status' THEN 'Created' ELSE 'Automated'
		END AS nueva
FROM cambios
)
,
ajustada AS
(
	SELECT DISTINCT issue_key,
    fecha,
    anterior,
    CASE
			WHEN nueva = 'Created' THEN COALESCE(
				LEAD(anterior) OVER (
					PARTITION BY issue_key
					ORDER BY fecha
				),
				COALESCE(
					(
						SELECT CASE
								WHEN strpos(labels, 'manual') > 0 THEN 'Manual'
								WHEN strpos(labels, 'NA') > 0 THEN 'NA'
								WHEN strpos(labels, 'pending') > 0 THEN 'Pending'
								WHEN strpos(labels, 'Pending') > 0 THEN 'Pending' ELSE 'Automated'
							END AS estatus
						FROM "dev_hudi_rwz_cfn"."jira_rwz_issues"
						WHERE key = issue_key
					),
					'Automated*'
				)
			) ELSE nueva
		END AS nueva
FROM cambios2
)
,
eliminados AS
(
	SELECT a.key AS issue_key,
    FROM_unixtime(a.loaded_date / 1000000) AS fecha,
    CASE
			WHEN strpos(labels, 'manual') > 0 THEN 'Manual'
			WHEN strpos(labels, 'NA') > 0 THEN 'NA' 
			WHEN strpos(labels, 'pending') > 0 THEN 'Pending'
			WHEN strpos(labels, 'Pending') > 0 THEN 'Pending' ELSE 'Automated'
		END AS anterior,
    'X' AS nueva
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" a
    INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" b ON a.issuetype_id = b.id
WHERE b.name = 'Xray Test'
    AND a.project_key = 'TS'
    AND a.status_dl = 'false'
)
,
consolidado0 AS
    (
        SELECT *
    FROM ajustada
    WHERE anterior != nueva
UNION ALL
    SELECT *
    FROM eliminados
)
,
ajustada2 AS
(
	SELECT a.key AS issue_key,
    date_parse(substr(a.created, 1, 19), '%Y-%m-%dT%H:%i:%s') AS fecha,
    'X' AS anterior,
    CASE
			WHEN strpos(labels, 'manual') > 0 THEN 'Manual'
			WHEN strpos(labels, 'pending') > 0 THEN 'Pending'
			WHEN strpos(labels, 'Pending') > 0 THEN 'Pending'
			WHEN strpos(labels, ' NA ') > 0 THEN 'NA' ELSE 'Automated'
		END AS nueva
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" a
    INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" b ON a.issuetype_id = b.id
WHERE b.name = 'Xray Test'
    AND a.project_key = 'TS'
    AND a.status_dl = 'true'
    AND a.key NOT IN (
			SELECT DISTINCT issue_key
    FROM consolidado0
		)
)
,
consolidado AS
    (
        SELECT *
    FROM ajustada2
UNION ALL
    SELECT *
    FROM consolidado0
)
,
salidAS AS
(
	SELECT f.fin_mes AS fecha,
    issue_key,
    anterior AS nueva,
    SUM(
			CASE
				WHEN c.fecha >= f.inicio_mes
        AND c.fecha <= f.fin_mes
        AND anterior != 'X' THEN -1 ELSE 0
			END
		) AS por_mes,
    SUM(
			CASE
				WHEN c.fecha <= f.fin_mes
        AND anterior != 'X' THEN -1 ELSE 0
			END
		) AS acumulado
FROM consolidado c
    JOIN fechAS f on (c.fecha <= f.fin_mes)
GROUP BY 1,
		2,
		3
)
,
entradAS AS
(
	SELECT f.fin_mes AS fecha,
    issue_key,
    nueva,
    SUM(
			CASE
				WHEN c.fecha >= f.inicio_mes
        AND c.fecha <= f.fin_mes
        AND nueva != 'X' THEN 1 ELSE 0
			END
		) AS por_mes,
    SUM(
			CASE
				WHEN c.fecha <= f.fin_mes
        AND nueva != 'X' THEN 1 ELSE 0
			END
		) AS acumulado
FROM consolidado c
    JOIN fechAS f on (c.fecha <= f.fin_mes)
GROUP BY 1,
		2,
		3
)
,
resultado AS
(
	SELECT e.fecha,
    e.issue_key,
    i.components,
    i.customfield_11266 AS product,
    e.nueva AS categoria,
    COALESCE(e.por_mes, 0) AS entrada_mes,
    COALESCE(s.por_mes, 0) AS salida_mes,
    COALESCE(e.acumulado, 0) AS entradAS_acumuladAS,
    COALESCE(s.acumulado, 0) AS salidAS_acumuladAS
FROM entradAS e
    LEFT JOIN salidAS s ON (
			e.fecha = s.fecha
        AND e.issue_key = s.issue_key
        AND e.nueva = s.nueva
		)
    INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issues" i ON (i.key = e.issue_key AND i.status_dl = 'true') -- quita los eliminados?*
)
,
teams AS
(
	SELECT DISTINCT components,
    customfield_11396 AS team
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues"
WHERE project_key = 'TS'
    AND issuetype_id = '10457'
)
SELECT r.fecha,
    r.components,
    r.categoria,
    r.product,
    sum(r.entrada_mes) AS entrada_mes,
    sum(r.salida_mes) AS salida_mes,
    sum(r.entradAS_acumuladAS) AS entradAS_acumuladAS,
    sum(r.salidAS_acumuladAS) AS salidAS_acumuladAS,
    t.team
FROM resultado r
    LEFT JOIN teams t on (r.components = t.components)
GROUP BY 1,
	2,
	3,
	4,
	9