SELECT spend_date, campaign, channel, clicks, spend_amount
FROM {{ source('silver', 'marketing_spend') }}