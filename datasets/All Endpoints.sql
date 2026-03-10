SELECT a.key,
	created,
	a.components,
	a.customfield_11266 AS product,
	a.summary,
	a.customfield_11396 as team,
	(
		SELECT COUNT(c.key)
		FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" c
		WHERE c.labels LIKE '%COT%'
			AND c.labels LIKE CONCAT('%EP_', a.key, '%')
	) AS COT,
	a.customfield_12333 as COT_status,
	(
		SELECT COUNT(c.key)
		FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" c
		WHERE c.labels LIKE '%FUT%'
			AND c.labels LIKE CONCAT('%EP_', a.key, '%')
	) AS FUT,
	a.customfield_12334 as FUT_status,
	a.status_dl as active,
	a.loaded_date as fechaCarga
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" a
	INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" b ON a.issuetype_id = b.id
WHERE b.name = 'Test Set'
	AND a.key LIKE 'TS_%'
	AND a.status_dl = 'true'
	AND a.components is not null
	AND a.components != 'CAPEX/OPEX'
	AND a.customfield_11266 is not null