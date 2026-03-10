SELECT b.name as proyecto,
	a.key as clave,
	a.status as estado,
	a.priority as prioridad,
	a.labels,
	a.customfield_11784 as razonDelBug,
	substring(a.created,1,10) as creacion,
	a.customfield_11773 as whereWasdetected,
	substring(a.resolved,1,10) as fechaResolucion
FROM "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" c
	inner join "dev_hudi_rwz_cfn"."jira_rwz_issues" a on (a.issuetype_id = c.id)
	inner join "dev_hudi_rwz_cfn"."jira_rwz_projects" b on (a.project_id = b.id)
	INNER JOIN "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" d on (a.issuetype_id = d.id)
	LEFT JOIN "dev_hudi_rwz_cfn"."jira_rwz_issues" e on (a.parent_key = e.key)
where a.status_dl = 'true'
	AND c.name in ('Bug', 'Story bug')