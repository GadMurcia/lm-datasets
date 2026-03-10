select date_format(
		date_parse(sprint.complete_date_str, '%d/%b/%y %H:%i %p'),
		'%d-%b-%y'
	) AS complete_date,
	sprint.name as Sprint_name,
	board.name as Board_name,
	project.name as project,
	sprint.id,
	sprint.total_compl_issues,
	(
		CASE
			WHEN sprint.sum_compl_issues_estim is null THEN 0 ELSE CAST(sprint.sum_compl_issues_estim AS DOUBLE)
		END
	) AS COMPLETED_POINTS,
	sprint.total_not_compl_issues,
	(
		CASE
			WHEN sprint.sum_not_compl_estim is null THEN 0 ELSE CAST(sprint.sum_not_compl_estim AS DOUBLE)
		END
	) AS NOT_COMPLETED_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'completedIssues'
				and report.type_name = 'Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as COMPLETED_STORIES,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'completedIssues'
				and report.type_name = 'Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as COMPLETED_STORIES_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
				and status_name = 'ON HOLD'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES_ONHOLD,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
				and status_name = 'ON HOLD'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES_ONHOLD_POINTS,
	coalesce(
		(
			select count(report.issue_report_type) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and status_name = 'ON HOLD'
			group by report.issue_report_type,
				report.sprint_id
		),
		0
	) as INCOMPLETED_ISSUES_ONHOLD,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and status_name = 'ON HOLD'
			group by report.issue_report_type,
				report.sprint_id
		),
		0
	) as INCOMPLETED_ISSUES_ONHOLD_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
				and status_name = 'QA validation'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES_QAVALIDATION,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
				and status_name = 'QA validation'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES_QAVALIDATION_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
				and status_name in ('To Do', 'In Progress')
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES_TODOANDWIP,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Story'
				and status_name in ('To Do', 'In Progress')
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_STORIES_TODOANDWIP_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'completedIssues'
				and report.type_name = 'Technical Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as COMPLETED_TECHNICAL_STORIES,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'completedIssues'
				and report.type_name = 'Technical Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as COMPLETED_TECHNICAL_STORIES_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Technical Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_TECHNICAL_STORIES,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Technical Story'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_TECHNICAL_STORIES_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'completedIssues'
				and report.type_name = 'Bug'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as COMPLETED_BUGS,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'completedIssues'
				and report.type_name = 'Bug'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as COMPLETED_BUGS_POINTS,
	coalesce(
		(
			select count(report.type_name) as count_type
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Bug'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_BUGS,
	coalesce(
		(
			select sum(CAST(current_estimate_statistic AS DOUBLE)) AS count_points
			from prd_hudi_rwz_cfn.jira_rwz_sprint_issue_report report
			where report.sprint_id = sprint.id
				and report.board_id = report.originboard_id
				and report.issue_report_type = 'issuesNotCompletedInCurrentSprint'
				and report.type_name = 'Bug'
			group by report.type_name,
				report.sprint_id,
				report.issue_report_type
		),
		0
	) as INCOMPLETED_BUGS_POINTS
FROM prd_hudi_rwz_cfn.jira_rwz_sprints sprint
	INNER JOIN prd_hudi_rwz_cfn.jira_rwz_boards board ON board.id = sprint.board_id
	INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_projects" project ON (board.project_id = project.id)
	and board.name NOT LIKE 'Multiple projects%'
where sprint.board_id = sprint.originboard_id