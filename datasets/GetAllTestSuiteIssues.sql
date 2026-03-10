SELECT *
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" ji
WHERE ji.project_key = 'TS' AND ji.issuetype_id = '10456' AND components LIKE '%ui%' AND status_dl = 'true'