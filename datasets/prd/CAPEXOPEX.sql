WITH points AS(
	SELECT DISTINCT i.key,
		SUBSTRING(i.resolved, 1, 7) AS mes,
		p.key AS project_key,
		p.name AS project,
		i.assignee,
		coalesce(z.issue_report_type, 'no sprint') as ReportType,
		CAST(customfield_10105 AS DOUBLE) AS points
	FROM "prd_hudi_rwz_cfn"."jira_rwz_issues" i
		INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" t ON t.id = i.issuetype_id
		INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_projects" p ON p.key = i.project_key
		LEFT JOIN "prd_hudi_rwz_cfn"."jira_rwz_sprint_issue_report" z ON (
			z.key = i.key
			AND z.originboard_id = z.board_id
			AND z.status_name = 'Done'
		)
)
SELECT mes,
	project_key,
	project,
	assignee,
	ReportType,
	sum(points) as puntosDelMes
FROM points
group by 1,
	2,
	3,
	4,
	5