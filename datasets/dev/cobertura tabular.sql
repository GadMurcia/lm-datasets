WITH
    exploded
    AS
    (
        SELECT components,
            TRIM(producto) AS producto,
            CASE
		WHEN a.labels LIKE '%;pending;%' OR a.labels LIKE '%pending;%' OR a.labels LIKE '%;pending%' OR a.labels LIKE 'pending' THEN 'pending'
		WHEN a.labels LIKE '%;NA;%' OR a.labels LIKE '%NA;%' OR a.labels LIKE '%;NA%' OR a.labels LIKE 'NA' THEN 'NA'
		WHEN a.labels LIKE '%;manual;%' OR a.labels LIKE '%manual;%' OR a.labels LIKE '%;manual%' OR a.labels LIKE 'manual' THEN 'manual' ELSE 'Automated'
	END AS status,
            1.0 / CARDINALITY(REGEXP_SPLIT(a.customfield_11266, ';')) AS peso
        FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" a
            INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" b ON b.id = a.issuetype_id
		CROSS JOIN UNNEST(REGEXP_SPLIT(a.customfield_11266, ';')) AS t (producto)
        WHERE a.project_key = 'TS'
            AND b.name = 'Xray Test'
            AND a.customfield_11266 IS NOT NULL
            AND a.components LIKE '%-ui'
            AND a.status_dl = 'true'
    )
SELECT components,
    producto,
    ROUND(SUM(peso), 2) AS total_issues,
    ROUND(
		SUM(
			CASE
				WHEN status = 'NA' THEN peso ELSE 0
			END
		),
		2
	) AS issues_NA,
    ROUND(
		SUM(
			CASE
				WHEN status = 'manual' THEN peso ELSE 0
			END
		),
		2
	) AS issues_manual,
    ROUND(
		SUM(
			CASE
				WHEN status = 'pending' THEN peso ELSE 0
			END
		),
		2
	) AS issues_pending,
    ROUND(
		SUM(
			CASE
				WHEN status = 'Automated' THEN peso ELSE 0
			END
		),
		2
	) AS issues_automatizados
FROM exploded
GROUP BY 1,2
ORDER BY total_issues DESC