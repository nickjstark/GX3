SELECT DISTINCT
    wsg.schedule_group_name sg,
    MAX(model_number) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) sched_model,
    MAX(we.wip_entity_name) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) wo,
    MAX(wdj.build_sequence) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) bs,
    MAX(wdj.scheduled_start_date) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) sched,
    MAX(wdj.start_quantity) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) ord_qty,
    MAX(wdj.quantity_completed) OVER (partition BY wsg.schedule_group_name, we.wip_entity_name) quantity_completed

FROM
    bwc_production bp
JOIN wip.wip_entities we
ON
    we.wip_entity_name = bp.work_order_number
    AND we.organization_id = bp.organization_id
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
    wdj.scheduled_start_date <= NEXT_DAY(TRUNC(sysdate), 'MONDAY') - 7                                                    -- Get everything starting from last Monday and previous (add Wednesday?)
    AND bp.production_line IN ('COM1', 'REP', 'WIP', 'PEND','HSS','HSE','HSG')                                            -- Remove unnecessary production lines
    AND bp.organization_id = 101
    AND wdj.quantity_completed != wdj.start_quantity                                                                      -- Get anything that isn't complete
    AND wdj.status_type = 3 OR wdj.status_type = 6                                                                        -- status_type 3 = Released and 6 = On Hold

ORDER BY
    sg,
    bs;
