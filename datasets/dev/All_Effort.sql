SELECT b.name as tipo,
	c.name as project,
	a.key as clave,
	a.assignee as aignado,
	a.customfield_10103 as sprint,
	a.customfield_11266 as producto,
	a.created as creacion,
	a.updated as actualizado,
	a.priority as prioridad,
	a.components as componentes,
	c.projectcategory_name as categoria,
	a.status as estado,
	a.customfield_11440 as QA_Asignado,
	a.labels as etiquetas,
	a.resolved as fechaResolucion,
	a.customfield_11433 as RealEndDate,
	a.customfield_11798 as TestExecutionSource,
	a.customfield_11609 as TotalQAStoryPoints,
	a.customfield_11603 as QAStoryPoints,
	A.customfield_10105 AS StoryPoints
FROM "dev_hudi_rwz_cfn"."jira_rwz_issues" a
	inner join "dev_hudi_rwz_cfn"."jira_rwz_issuetypes" b on (a.issuetype_id = b.id)
	inner join "dev_hudi_rwz_cfn"."jira_rwz_projects" c on (a.project_id = c.id)
where a.status_dl = 'true'