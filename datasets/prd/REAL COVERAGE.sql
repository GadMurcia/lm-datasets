SELECT i.key,
    i.components,
    SUBSTRING(i.created, 1, 10) as created,
    i.summary,
    i.customfield_11800 as passed,
    i.customfield_11801 as totalExecuted,
    i.customfield_12003 as reason,
    i.customfield_12002 as origenCall,
    i.customfield_11759 as resolution,
    i.customfield_11266 as product,
    i.customfield_12135 as stamp
FROM "prd_hudi_rwz_cfn"."jira_rwz_issues" i
    INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_issuetypes" t on (i.issuetype_id = t.id)
where t.name in ('Results')
    AND project_key = 'TS'
    and status_dl = 'true'
    AND customfield_12002 IN ('Manual execution', 'Automated execution')
    AND customfield_12135 != ''