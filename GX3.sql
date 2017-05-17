SELECT
    sched_group,
    build_sequence,
    scheduled_start_date,
    schedule_start_day,
    total_qty - qty_completed remaining,
    SUM(total_qty - qty_completed) OVER(PARTITION BY sched_group ORDER BY build_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) total_open,
    cap,
    round(SUM(total_qty - qty_completed) OVER(PARTITION BY sched_group ORDER BY build_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) / (cap *1.009),1) days_at_8,
    round(SUM(total_qty - qty_completed) OVER(PARTITION BY sched_group ORDER BY build_sequence RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) / (cap *1.2332),1) days_at_10

FROM
    (
        SELECT DISTINCT
            wsg.schedule_group_name sched_group,
            substr(build_sequence,1,5) build_sequence,
            to_char(MIN(wdj.scheduled_start_date) OVER(PARTITION BY wsg.schedule_group_name, substr( build_sequence, 1, 5 )),'DAY DD-MON-YYYY') scheduled_start_date,
                CASE TO_CHAR(MIN(wdj.scheduled_start_date) OVER(PARTITION BY wsg.schedule_group_name,substr(build_sequence,1,5)),'D')
                    WHEN '2'   THEN 'A'
                    WHEN '3'   THEN 'A'
                    WHEN '4'   THEN 'B'
                    WHEN '5'   THEN 'B'
                    WHEN '6'   THEN 'B'
                    ELSE 'WKND'
                END schedule_start_day,
            SUM(wdj.start_quantity) OVER(PARTITION BY wsg.schedule_group_name, substr(build_sequence,1,5)) total_qty,
            SUM(wdj.quantity_completed) OVER(PARTITION BY wsg.schedule_group_name,substr(build_sequence,1,5)) qty_completed,
--    wdj.scheduled_start_date
            TO_NUMBER(wsg.attribute6) cap
        FROM
            wip.wip_move_transactions wmt
            JOIN wip.wip_entities we ON
                we.wip_entity_name = wmt.wip_entity_id
            AND we.organization_id = wmt.organization_id
            AND we.organization_id = 101
            JOIN wip.wip_discrete_jobs wdj ON wdj.wip_entity_id = we.wip_entity_id
            JOIN wip.wip_schedule_groups wsg ON
                wsg.schedule_group_id = wdj.schedule_group_id
            AND wsg.schedule_group_name IN ('COMMASSY','HS GAS','HS ELEC','SPEC')
        WHERE
                wdj.scheduled_start_date >= next_day(trunc(SYSDATE),'MONDAY') - 21
            AND wmt.organization_id = 101
            AND wdj.quantity_completed != wdj.start_quantity                                                -- Get anything that isn't complete
            AND wdj.status_type IN (3,6)                                                                   -- status_type 3 = Released and 6 = On Hold
    )
ORDER BY
    sched_group,
    build_sequence,
    schedule_start_day;

SELECT DISTINCT
    wsg.schedule_group_name sched_group,
        SUM(CASE WHEN to_number(TO_CHAR(wmt.transaction_date,'HH24') ) BETWEEN 4 AND 15 THEN wmt.transaction_quantity ELSE 0 END) OVER (PARTITION BY WSG.SCHEDULE_GROUP_NAME) FIRST_SHFT,
        SUM(CASE WHEN to_number(TO_CHAR(wmt.transaction_date,'HH24') ) BETWEEN 4 AND 15 THEN 0 ELSE wmt.transaction_quantity  END) OVER (PARTITION BY WSG.SCHEDULE_GROUP_NAME) SCND_SHFT,
    SUM(wmt.transaction_quantity) OVER(PARTITION BY wsg.schedule_group_name) completed
FROM
    wip_move_transactions wmt
    JOIN wip_discrete_jobs wdj ON
        wdj.wip_entity_id = wmt.wip_entity_id
    AND wdj.organization_id = wmt.organization_id
    AND wdj.organization_id = 101
    JOIN wip_schedule_groups wsg ON
        wsg.organization_id = wdj.organization_id
    AND wsg.schedule_group_id = wdj.schedule_group_id
    AND wsg.schedule_group_name IN ('COMMASSY','HS GAS','HS ELEC','SPEC','CONVERSIONS','COMMCONV')
WHERE
        wmt.organization_id = 101
    AND wmt.transaction_date BETWEEN trunc(SYSDATE - 1) + 3 / 24 AND trunc(SYSDATE) + 3 / 24
ORDER BY 1;
