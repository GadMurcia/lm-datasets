SELECT status,
	count(status) as total
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues"
where issuetype_id = '10103'
	and status_dl = 'true'
	and status not in ('Done', 'Cancelled')
group by status
order by status asc