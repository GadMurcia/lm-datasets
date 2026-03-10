SELECT a.customfield_11789 as authorizedby, b.name as project, count(b.name) as total
FROM "dev_hudi_rwz_cfn"."jira_poc_rwz_issues" a
	join "dev_hudi_rwz_cfn"."jira_rwz_projects" b on (a.project_key = b.key)
where a.issuetype_id = '10103' and a.status_dl = 'true' and a.status in ('Request 1st extension')
group by  a.customfield_11789, b.name
order by project asc