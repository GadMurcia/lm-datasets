WITH
    r
    as
    (
        SELECT
            key,
            status_id,
            customfield_10806_value as ChangeResolution,
            TRIM(observation) AS fieldsWithQAObservations,
            customfield_10400_value AS reporterArea,
            substr(customfield_10700,1,19) as executionTime,
            substr(resolved, 1, 19) AS resolved,
            substr(customfield_11203, 1, 19) AS changeCompletionDate
        FROM "prd_hudi_rwz_cfn"."jira_rwz_rfcs",
            UNNEST(split(customfield_11602_value, ';')) AS t (observation)
        WHERE customfield_11602_value IS NOT NULL
    )
Select rfc.*, b.name as general_status
from r as rfc
    INNER JOIN "prd_hudi_rwz_cfn"."jira_rwz_status" b ON (rfc.status_id = b.id)