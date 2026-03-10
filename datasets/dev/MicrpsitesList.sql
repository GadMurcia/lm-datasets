WITH exploded_components AS (
	SELECT TRIM(component) AS component
	FROM "dev_hudi_rwz_cfn"."jira_rwz_issues",
		UNNEST(SPLIT(components, ';')) AS t(component)
	WHERE issuetype_id = '10456'
)
SELECT DISTINCT component
FROM exploded_components
WHERE component LIKE '%-ui'
ORDER BY component ASC