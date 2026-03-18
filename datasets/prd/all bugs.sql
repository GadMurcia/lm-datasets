SELECT c.name AS tipo,
    a.project_key,
    a.labels,
    a.reporter,
    b.name AS proyecto,
    a.key AS clave,
    a.components AS componente,
    a.priority AS prioridad,
    a.status AS estado,
    a.customfield_11266 AS producto,
    a.customfield_11789 AS authorizedby,
    b.projectcategory_name AS categoria,
    SUBSTRING(a.created, 1, 10) AS creacion,
    SUBSTRING(a.updated, 1, 10) AS actualizacion,
    SUBSTRING(a.resolved, 1, 10) AS fechaResolucion,
    a.customfield_11788 AS fechaResolucionEsperada,
    a.customfield_11787 AS waiverSolicitados,
    a.assignee AS PersonaACaorgo,
    a.customfield_11784 AS razonDelBug,
    a.customfield_11773 AS whereWasdetected,
    a.customfield_10103 AS sprint,
    a.customfield_11798 AS TestExecutionSource,
    a.customfield_11759 AS ResultType,
    a.summary,
    a.parent_key,
    e.assignee AS parentAssignee,
    e.status AS parentStatus,
    a.customfield_10105 AS StoryPoints
FROM "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" c
    INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issues" a ON (a.issuetype_id = c.id)
    INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_projects" b ON (a.project_id = b.id)
    INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" d ON (a.issuetype_id = d.id)
    LEFT JOIN "prd_hudi_rwz_cfn"."jira_rwz_issues" e ON (a.parent_key = e.key)
WHERE a.status_dl = 'true'