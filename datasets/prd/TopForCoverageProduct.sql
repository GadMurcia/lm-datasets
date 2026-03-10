WITH build AS (
	SELECT resolved,
		key,
		customfield_12002 AS origen_call,
		customfield_11266 AS products
	FROM "prd_hudi_rwz_cfn"."jira_rwz_issues"
	WHERE issuetype_id = '10475'
		AND status_dl = 'true'
		AND customfield_12002 IN ('Manual execution', 'Automated execution')
		AND customfield_12135 != ''
),
separated_products AS (
	SELECT resolved,
		key,
		origen_call,
		product
	FROM build
		CROSS JOIN UNNEST(SPLIT(products, ';')) AS t (product)
)
SELECT resolved,
	key,
	origen_call,
	product
FROM separated_products