SELECT SUBSTRING(resolved, 1, 10) as resolved,
    key,
    customfield_12002 AS origen_call,
    TRIM(component) AS component
FROM "prd_hudi_rwz_cfn"."jira_rwz_issues",
    UNNEST(SPLIT(components, ';')) AS t(component)
WHERE issuetype_id = '10475'
    AND status_dl = 'true'
    AND customfield_12002 IN ('Manual execution', 'Automated execution')
    AND customfield_12135 != ''
ORDER BY key,
	component