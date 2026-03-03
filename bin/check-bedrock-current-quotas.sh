aws service-quotas list-service-quotas --service-code bedrock --profile usf478 --query 'Quotas[?contains(QuotaName, `Claude`) || contains(QuotaName, `Anthropic`)]'
