SELECT DISTINCT
    CASE
      WHEN TO_CHAR(wdj.scheduled_start_date, 'D') IN(2, 3) THEN 'MONDAY_SCHEDULE'
      WHEN TO_CHAR(wdj.scheduled_start_date, 'D') IN(4, 5, 6) THEN 'WEDNESDAY_SCHEDULE'
      ELSE 'WEEKEND_SCHEDULE'
    END
    wsg.schedule_group_name sched_group,
    MAX(model_number) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) sched_model,
    MAX(we.wip_entity_name) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) wo,
    MAX(wdj.build_sequence) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) build_sequence,
    MAX(wdj.scheduled_start_date) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) sched,
    MAX(wdj.start_quantity) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) ord_qty,
    MAX(wdj.quantity_completed) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) quantity_completed

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
    --bp.date_time_completion BETWEEN sysdate - 1 AND sysdate
    wdj.scheduled_start_date <= NEXT_DAY(TRUNC(sysdate), 'MONDAY') - 5                                                    -- Get everything starting from last Monday and previous (add Wednesday?)
    AND bp.production_line IN ('COM1', 'HSS','HSE','HSG')                                                                 -- Are there additional lines to monitor?
    AND wmt.organization_id = 101
    AND wdj.quantity_completed != wdj.start_quantity                                                                      -- Get anything that isn't complete
    AND wdj.status_type = 3 OR wdj.status_type = 6                                                                        -- status_type 3 = Released and 6 = On Hold

ORDER BY
    sched_group,
    build_sequence;
