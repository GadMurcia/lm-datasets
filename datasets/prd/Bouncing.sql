WITH
    filtered_changes
    AS
    (
        SELECT issue_key,
            created,
            CASE
			WHEN items_fromstring LIKE '%To Do%' THEN 'To Do'
			WHEN items_fromstring LIKE '%In Progress%' THEN 'In Progress'
			WHEN items_fromstring LIKE '%QA validation%' THEN 'QA validation'
			WHEN items_fromstring LIKE '%ON HOLD%' THEN 'ON HOLD'
			WHEN items_fromstring LIKE '%Troubleshooting%' THEN 'Troubleshooting'
			WHEN items_fromstring LIKE '%Done%' THEN 'Done' ELSE NULL
		END AS previous_status,
            CASE
			WHEN items_tostring LIKE '%To Do%' THEN 'To Do'
			WHEN items_tostring LIKE '%In Progress%' THEN 'In Progress'
			WHEN items_tostring LIKE '%QA validation%' THEN 'QA validation'
			WHEN items_tostring LIKE '%ON HOLD%' THEN 'ON HOLD'
			WHEN items_tostring LIKE '%Troubleshooting%' THEN 'Troubleshooting'
			WHEN items_tostring LIKE '%Done%' THEN 'Done' ELSE NULL
		END AS new_status
        FROM "prd_hudi_rwz_cfn"."jira_rwz_changelog_histories"
        WHERE items_field LIKE '%status%'
    ),
    status_changes
    AS
    (
        SELECT issue_key,
            created AS change_date,
            previous_status,
            new_status
        FROM filtered_changes
        WHERE previous_status IS NOT NULL
            OR new_status IS NOT NULL
    ),
    status_durations
    AS
    (
        SELECT sc.issue_key,
            sc.previous_status,
            sc.new_status,
            sc.change_date,
            LEAD(sc.change_date, 1, jri.customfield_10809) OVER (
			PARTITION BY sc.issue_key
			ORDER BY sc.change_date
		) AS next_change_date
        FROM status_changes sc
            INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issues" jri ON sc.issue_key = jri.key
    ),
    status_summary
    AS
    (
        SELECT issue_key,
            new_status AS status,
            COUNT(*) AS status_count,
            SUM(
			GREATEST(
				(
					to_unixtime(from_iso8601_timestamp(next_change_date)) - to_unixtime(from_iso8601_timestamp(change_date))
				) / 86400.0,
				0
			)
		) AS total_days_in_status
        FROM status_durations
        GROUP BY issue_key,
		new_status
    ),
    first_leave_todo
    AS
    (
        SELECT issue_key,
            MIN(change_date) AS first_leave_date
        FROM status_changes
        WHERE previous_status = 'To Do'
            AND new_status != 'To Do'
        GROUP BY issue_key
    ),
    issues_without_todo
    AS
    (
        SELECT jri.key AS issue_key
        FROM "prd_hudi_rwz_cfn"."jira_rwz_issues" jri
        WHERE jri.key NOT IN (
			SELECT issue_key
        FROM status_summary
        WHERE status = 'To Do'
		)
    ),
    preview
    AS
    (
                    SELECT t.name AS tipo,
                jri.key,
                pr.name AS proyecto,
                jri.resolved AS resolution_date,
                COALESCE(ss.status, 'To Do') AS status,
                COALESCE(ss.status_count, 0) AS status_count,
                COALESCE(ss.total_days_in_status, 0) AS total_days_in_status,
                ABS(
			(
				to_unixtime(from_iso8601_timestamp(flt.first_leave_date)) - to_unixtime(from_iso8601_timestamp(jri.created))
			) / 86400.0
		) AS days_to_leave_todo
            FROM "prd_hudi_rwz_cfn"."jira_rwz_issues" jri
                LEFT JOIN status_summary ss ON jri.key = ss.issue_key
                LEFT JOIN first_leave_todo flt ON jri.key = flt.issue_key
                INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_projects" pr ON jri.project_id = pr.id
                INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" t ON jri.issuetype_id = t.id
        UNION ALL
            SELECT t.name AS tipo,
                jri.key,
                pr.name AS proyecto,
                jri.resolved AS resolution_date,
                'To Do' AS status,
                0 AS status_count,
                0 AS total_days_in_status,
                ABS(
			(
				to_unixtime(from_iso8601_timestamp(flt.first_leave_date)) - to_unixtime(from_iso8601_timestamp(jri.created))
			) / 86400.0
		) AS days_to_leave_todo
            FROM issues_without_todo iwt
                INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issues" jri ON iwt.issue_key = jri.key
                INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_projects" pr ON jri.project_id = pr.id
                INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" t ON jri.issuetype_id = t.id
                LEFT JOIN first_leave_todo flt ON jri.key = flt.issue_key
    )
SELECT p.tipo,
    p.key,
    p.proyecto,
    p.resolution_date AS fecha,
    p.status,
    CASE
		WHEN (
			p.status LIKE 'To Do'
        AND p.days_to_leave_todo > 0
		) THEN (p.status_count + 1) ELSE p.status_count
	END AS Veces_En_El_Estado,
    CASE
		WHEN p.status LIKE 'To Do' THEN (p.total_days_in_status + p.days_to_leave_todo) ELSE p.total_days_in_status
	END AS Dias_En_El_Estado
FROM preview p
order by key