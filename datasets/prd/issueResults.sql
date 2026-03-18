SELECT key,
    components,
    customfield_11266 as products,
    summary,
    customfield_11800 as passed,
    customfield_11801 as ejecutados,
    customfield_11802 as score,
    SUBSTRING(updated, 1, 10) as updated,
    customfield_11759 as resolution,
    customfield_12002 as origen_call,
    customfield_12135 as buildId,
    customfield_12003 as result
FROM "prd_hudi_rwz_cfn"."jira_rwz_issues" 
where issuetype_id = '10475' 
    and customfield_11802 >= '0' 
    and status_dl = 'true' 
    and customfield_12002 in ('Manual execution', 'Automated execution')
    and customfield_12135 != '' 
order by customfield_12002 asc, key