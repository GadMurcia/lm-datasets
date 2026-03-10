WITH sprints AS (
	SELECT distinct id,
		board_id
	FROM "prd_hudi_rwz_cfn"."jira_rwz_sprints"
),
notCompletedQA AS (
	SELECT m.sprint_id,
		m.board_id,
		COUNT(status_name) AS QANotCompleted
	FROM "prd_hudi_rwz_cfn"."jira_rwz_sprint_issue_report" m
		inner join sprints n on (
			m.sprint_id = n.id
			and m.board_id = n.board_id
		)
	WHERE issue_report_type = 'issuesNotCompletedInCurrentSprint'
		AND status_name = 'QA validation'
		and type_name = 'Story'
	GROUP BY sprint_id,
		m.board_id
)
SELECT a.id as sprint_id,
	a.name as sprint_name,
	a.state as sprint_state,
	cast(
		date_parse(
			regexp_replace(a.start_date, 'T.*', ''),
			'%Y-%m-%d'
		) AS date
	) AS start_date,
	cast(
		date_parse(
			regexp_replace(a.complete_date, 'T.*', ''),
			'%Y-%m-%d'
		) AS date
	) AS complete_date,
	b.name as board_name,
	b.type as board_type,
	c.name as project_name,
	c.key as project_id,
	c.projectcategory_name as project_category,
	CAST(a.total_compl_issues AS DOUBLE) AS compl_issues,
	CAST(a.total_not_compl_issues AS DOUBLE) AS not_compl_issues,
	d.QANotCompleted AS issueNotCompletedQA
FROM "prd_hudi_rwz_cfn"."jira_rwz_sprints" a
	INNER JOIN (
		SELECT sprint_id,
			board_id,
			QANotCompleted
		FROM notCompletedQA
	) d ON (
		a.id = d.sprint_id
		and a.board_id = d.board_id
	)
	INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_boards" b ON (a.board_id = b.id)
	INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_projects" c on (b.project_id = c.id)