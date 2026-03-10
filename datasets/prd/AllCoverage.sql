WITH cambios AS (
	SELECT a.issue_key,
		substr(a.created, 1, 10) AS mes,
		a.items_fromstring,
		a.items_tostring,
		a.items_field,
		b.components,
		b.customfield_11266 as product
	FROM "prd_hudi_rwz_cfn"."jira_rwz_changelog_histories" a
		INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issues" b ON a.issue_key = b.key
		INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" c ON b.issuetype_id = c.id
	WHERE c.name = 'Xray Test'
		AND b.project_key = 'TS'
		AND (
			a.items_field LIKE '%labels%'
			OR (
				a.items_field = 'resolution;status'
				AND a.items_fromstring = 'null;To Do'
				AND a.items_tostring = 'Done;Done'
			)
		)
),
cambios2 AS (
	SELECT issue_key,
		mes,
		CASE
			WHEN strpos(items_tostring, ' NA ') > 0 THEN 'NA'
			WHEN strpos(items_tostring, 'pending') > 0 THEN 'Pending'
			WHEN strpos(items_tostring, 'manual') > 0 THEN 'Manual'
			WHEN items_field = 'resolution;status' THEN 'Created' ELSE 'Automated'
		END AS nueva,
		CASE
			WHEN strpos(items_fromstring, ' NA ') > 0 THEN 'NA'
			WHEN strpos(items_fromstring, 'pending') > 0 THEN 'Pending'
			WHEN strpos(items_fromstring, 'manual') > 0 THEN 'Manual'
			WHEN items_field = 'resolution;status' THEN '0' ELSE 'Automated'
		END AS anterior,
		components,
		product
	FROM cambios
),
cambios_filtrados AS (
	SELECT DISTINCT issue_key,
		mes,
		anterior,
		nueva,
		components,
		product
	FROM cambios2
	WHERE anterior != nueva
)
SELECT *
FROM cambios_filtrados
UNION ALL
SELECT a.key AS issue_key,
	date_format(
		from_unixtime(a.loaded_date / 1000000),
		'%Y-%m-%d'
	) AS mes,
	'0' AS anterior,
	'Deleted' AS nueva,
	a.components,
	a.customfield_11266 as product
FROM "prd_hudi_rwz_cfn"."jira_rwz_issues" a
	INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" b ON a.issuetype_id = b.id
WHERE b.name = 'Xray Test'
	AND a.project_key = 'TS'
	AND a.status_dl = 'false'
ORDER BY issue_key