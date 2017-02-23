SELECT DISTINCT
    wsg.schedule_group_name sched_group,
    substr(build_sequence,1,5) build_sequence,
    MAX(wdj.scheduled_start_date) OVER (PARTITION BY wsg.schedule_group_name, substr(build_sequence,1,5)) scheduled_start_date,
    CASE WHEN TO_CHAR(wdj.scheduled_start_date, 'D') IN(2, 3) THEN 'MONDAY_SCHEDULE'
         WHEN TO_CHAR(wdj.scheduled_start_date, 'D') IN(4, 5, 6) THEN 'WEDNESDAY_SCHEDULE'
         ELSE 'WEEKEND_SCHEDULE' END AS schedule_start_day,
    SUM(wdj.start_quantity) OVER (PARTITION BY wsg.schedule_group_name, substr(build_sequence,1,5)) total_qty,
    SUM(wdj.quantity_completed) OVER (PARTITION BY wsg.schedule_group_name, substr(build_sequence,1,5)) quantity_completed,
    wdj.scheduled_start_date

FROM
    wip.wip_move_transactions wmt
JOIN wip.wip_entities we
ON
    we.wip_entity_name = wmt.wip_entity_id
    AND we.organization_id = wmt.organization_id
    AND we.organization_id = 101
JOIN wip.wip_discrete_jobs wdj
ON
    wdj.wip_entity_id = we.wip_entity_id
JOIN wip.wip_schedule_groups wsg
ON
    wsg.schedule_group_id = wdj.schedule_group_id
    AND wsg.schedule_group_name IN ('COMMASSY', 'HS GAS','HS ELEC','SPEC')
WHERE
    wdj.scheduled_start_date >= NEXT_DAY(TRUNC(sysdate), 'MONDAY') - 7
    AND wmt.organization_id = 101
    AND wdj.quantity_completed != wdj.start_quantity                                                -- Get anything that isn't complete
    AND wdj.status_type IN (3, 6)                                                                   -- status_type 3 = Released and 6 = On Hold

ORDER BY
    sched_group,
    build_sequence,
    schedule_start_day;
