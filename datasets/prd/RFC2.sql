WITH r as (
  SELECT 
  key,
  status_id,
  customfield_10806_value as ChangeResolution,
  TRIM(product) AS ProductOrApplication,
  customfield_10400_value AS reporterArea,
  substr(customfield_10700,1,19) as executionTime,
  substr(resolved, 1, 19) AS resolved,
  substr(customfield_11203, 1, 19) AS changeCompletionDate,
  date_diff('day', date_parse(substr(customfield_11203, 1, 10), '%Y-%m-%d'), date_parse(substr(resolved, 1, 10), '%Y-%m-%d')) AS daysDifference
FROM "prd_hudi_rwz_cfn"."jira_rwz_rfcs",
     UNNEST(split(customfield_11373_value, ';')) AS t (product)
WHERE customfield_11203 IS NOT NULL
  AND resolved IS NOT NULL
)

 Select rfc.*, b.name as general_status from r as rfc
	INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_status" b ON (rfc.status_id = b.id)