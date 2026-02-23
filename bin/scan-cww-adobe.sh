aws logs filter-log-events \
    --log-group-name "/ecs/pdf-remediation/adobe-autotag" \
    --start-time $(date -d '10 minutes ago' +%s)000 \
    --filter-pattern "?\"Acquired slot\" ?\"Released slot\" ?\"In-flight status\" ?\"At capacity\"" \
    --query 'events[*].message' \
    --output text

