SELECT b.name, a.priority, count(a.priority) as count
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" a
	join "dev_hudi_rwz_cfn"."jira_rwz_projects" b on (a.project_key = b.key)
where a.issuetype_id = '10103' and a.status_dl = 'true' and a.status not in ('Done', 'Cancelled') and b.projectcategory_name in ('Agile Development', 'Agile Testing')
group by b.name, a.priority
order by b.name asc, priority asc