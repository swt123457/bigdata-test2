package com.kye.utils;

public class SqlTest {
    public static final String sql0 = "select count(*) as cnt from r_vms_capacity_management_dept_kpis_daily_month_new_test;";
    public static final String sql1 = "with tb1 as (\n" +
            "SELECT\n" +
            "lv9_id AS departmentId, \n" +
            "SUM(car_Cnt) AS carCnt,\n" +
            "SUM(nogc_car_cnt) AS gccarCnt,\n" +
            "SUM(nogc_car_day_cnt) AS cardayCnt,\n" +
            "SUM(new_car_cnt) AS newCarCnt,\n" +
            "SUM(scraping_car_cnt) AS scrapingCarCnt, \n" +
            "SUM(quiting_car_cnt) AS quitingCarCnt,\n" +
            "SUM(scrap_car_cnt) AS scrapCarCnt,\n" +
            "SUM(non_cooperation_car_cnt) AS nonCooperationCarCnt,\n" +
            "SUM(valid_vehicle_mileage + invalid_vehicle_mileage) AS vehicleMileage,\n" +
            "SUM(valid_vehicle_mileage) AS alidVehicleMileage,\n" +
            "SUM(invalid_vehicle_mileage) AS invalidVehicleMileage,\n" +
            "SUM(subline_mileage) AS pickupDeliveryMileage,\n" +
            "SUM(vehicle_weight) AS operationWeight,\n" +
            "SUM(load_capacity) AS loadCapacity,\n" +
            "COALESCE(SUM(total_rate) / CAST(SUM(total_cnt) AS DECIMAL(18, 4)), 0.0) AS loadCapacityRate, \n" +
            "COALESCE(SUM(pre_total_rate) / CAST(SUM(pre_total_cnt) AS DECIMAL(18, 4)), 0.0) AS preLoadCapacityRate,\n" +
            "SUM(vehicle_income) AS predictIncome,\n" +
            "SUM(profit) AS profit,\n" +
            "SUM(task_ontime_qty) AS task_ontime_qty,\n" +
            "SUM(task_not_ontime_qty) AS task_not_ontime_qty,\n" +
            "SUM(fuel_amount) AS fuelVfolume,\n" +
            "SUM(CASE WHEN vehicle_attribute = '30' THEN oil_mileage ELSE valid_vehicle_mileage + invalid_vehicle_mileage END ) AS oil_mileage,\n" +
            "SUM(internal_fuel_amount) AS internal_fuel_amount,\n" +
            "SUM(external_fuel_amount) AS external_fuel_amount,\n" +
            "SUM(internal_oil_mileage) AS internal_oil_mileage,\n" +
            "SUM(external_oil_mileage) AS external_oil_mileage,\n" +
            "SUM(fule_cost) AS fule_cost,\n" +
            "SUM(CASE WHEN t.vehicle_attribute <> '30' THEN internal_diesel ELSE 0 END) AS internal_diesel,\n" +
            "SUM(CASE WHEN t.vehicle_attribute <> '30' THEN total_diesel ELSE 0 END) AS total_diesel,\n" +
            "CAST(SUM(liability_accident_flag - handle_privately_accident_flag) * 1000000 AS DECIMAL(22, 4)) / SUM(liability_accident_mileage) AS HundredKmFaultCnt,\n" +
            "SUM(maintenance_duration_cnt) AS maintenanceCnt,\n" +
            "SUM(maintenance_fee) AS maintenanceFee,\n" +
            "SUM(total_fee) AS totalCost,\n" +
            "SUM(pickup_batch_cnt + delivery_batch_cnt) AS batch_cnt,\n" +
            "SUM(pickup_batch_cnt) AS pickupBatchNum,\n" +
            "SUM(delivery_batch_cnt) AS deliveryBatchNum, \n" +
            "SUM(maintenance_cost + upkeep_cost + tire_cost + supporting_cost + advertising_cost) AS maintenanceCost,\n" +
            "SUM(insurance_cost + trial_cost + depreciation_fee) AS fixedCost,\n" +
            "SUM(accident_cnt) AS accidentsCnt,\n" +
            "SUM(injured_cnt) AS injuryCnt,\n" +
            "SUM(death_cnt) AS deathCnt,\n" +
            "ROUND(SUM(run_time),2) AS runTime,\n" +
            "SUM(idle_days) AS idleDays,\n" +
            "SUM(gc_income) AS gcIncome,\n" +
            "SUM(dh_income96) AS dhIncome96,\n" +
            "SUM(lldh_income96) AS lldhIncome96,\n" +
            "SUM(dh_income76) AS dhIncome76,\n" +
            "SUM(lldh_income76) AS lldhIncome76,\n" +
            "SUM(zh_income) AS zhIncome,\n" +
            "SUM(xh_income) AS xhIncome,\n" +
            "SUM(sl_income) AS slIncome,\n" +
            "SUM(el_income) AS elIncome,\n" +
            "SUM(tt_income) AS ttIncome,\n" +
            "SUM(mb_income) AS mbIncome,\n" +
            "CAST(SUM(pre_car_cnt) AS DECIMAL(18, 4)) AS periodCarCnt, \n" +
            "CAST(SUM(pre_car_cnt) AS DECIMAL(18, 4)) AS pre_car_cnt,\n" +
            "CAST(SUM(pre_nogc_car_cnt) AS DECIMAL(18, 4)) AS pre_nogc_car_cnt,\n" +
            "CAST(SUM(pre_nogc_car_day_cnt) AS DECIMAL(18, 4)) AS pre_cardayCnt,\n" +
            "SUM(pre_vehicle_mileage) AS periodVehicleMileage,\n" +
            "SUM(pre_fuel_amount) AS pre_fuel_amount,\n" +
            "SUM(CASE WHEN vehicle_attribute = '30' THEN pre_oil_mileage ELSE pre_vehicle_mileage END) AS pre_oil_mileage,\n" +
            "SUM(pre_internal_fuel_amount) AS pre_internal_fuel_amount,\n" +
            "SUM(pre_batch_cnt) AS pre_batch_cnt,\n" +
            "SUM(pre_subline_mileage) AS pre_subline_mileage,\n" +
            "SUM(pre_maintenance_fee) AS preMaintenanceFee,\n" +
            "SUM(pre_profit) AS preProfit,\n" +
            "SUM(pre_internal_diesel) AS pre_internal_diesel,\n" +
            "SUM(pre_total_diesel) AS pre_total_diesel,\n" +
            "CAST(SUM(pre_accident_cnt) AS DECIMAL(18, 4)) AS preAccidentsCnt,\n" +
            "ROUND(SUM(pre_run_time),2) AS preRunTime,\n" +
            "CAST(SUM(pre_idle_days) AS DECIMAL(18, 4)) AS preIdleDays,\n" +
            "SUM(pre_total_fee) AS preTotalCost\n" +
            "from (\n" +
            "SELECT * FROM r_vms_capacity_management_dept_kpis_daily_month_new_test\n" +
            "WHERE lv2_id = 22 AND department_org LIKE '%10%' \n" +
            "AND hierarchy_id LIKE '%/90/%' limit 10000\n" +
            ") as t\n" +
            "GROUP BY lv9_id\n" +
            "HAVING SUM(car_Cnt) > 0\n" +
            ")\n" +
            "select * from tb1\n" +
            "union all\n" +
            "select 'total' as total\n" +
            ",sum(carCnt)\n" +
            ",sum(gccarCnt)\n" +
            ",sum(cardayCnt)\n" +
            ",sum(newCarCnt)\n" +
            ",sum(scrapingCarCnt)\n" +
            ",sum(quitingCarCnt)\n" +
            ",sum(scrapCarCnt)\n" +
            ",sum(nonCooperationCarCnt)\n" +
            ",sum(vehicleMileage)\n" +
            ",sum(alidVehicleMileage)\n" +
            ",sum(invalidVehicleMileage)\n" +
            ",sum(pickupDeliveryMileage)\n" +
            ",sum(operationWeight)\n" +
            ",sum(loadCapacity)\n" +
            ",sum(loadCapacityRate)\n" +
            ",sum(preLoadCapacityRate)\n" +
            ",sum(predictIncome)\n" +
            ",sum(profit)\n" +
            ",sum(task_ontime_qty)\n" +
            ",sum(task_not_ontime_qty)\n" +
            ",sum(fuelVfolume)\n" +
            ",sum(oil_mileage)\n" +
            ",sum(internal_fuel_amount)\n" +
            ",sum(external_fuel_amount)\n" +
            ",sum(internal_oil_mileage)\n" +
            ",sum(external_oil_mileage)\n" +
            ",sum(fule_cost)\n" +
            ",sum(internal_diesel)\n" +
            ",sum(total_diesel)\n" +
            ",sum(HundredKmFaultCnt)\n" +
            ",sum(maintenanceCnt)\n" +
            ",sum(maintenanceFee)\n" +
            ",sum(totalCost)\n" +
            ",sum(batch_cnt)\n" +
            ",sum(pickupBatchNum)\n" +
            ",sum(deliveryBatchNum)\n" +
            ",sum(maintenanceCost)\n" +
            ",sum(fixedCost)\n" +
            ",sum(accidentsCnt)\n" +
            ",sum(injuryCnt)\n" +
            ",sum(deathCnt)\n" +
            ",sum(runTime)\n" +
            ",sum(idleDays)\n" +
            ",sum(gcIncome)\n" +
            ",sum(dhIncome96)\n" +
            ",sum(lldhIncome96)\n" +
            ",sum(dhIncome76)\n" +
            ",sum(lldhIncome76)\n" +
            ",sum(zhIncome)\n" +
            ",sum(xhIncome)\n" +
            ",sum(slIncome)\n" +
            ",sum(elIncome)\n" +
            ",sum(ttIncome)\n" +
            ",sum(mbIncome)\n" +
            ",sum(periodCarCnt)\n" +
            ",sum(pre_car_cnt)\n" +
            ",sum(pre_nogc_car_cnt)\n" +
            ",sum(pre_cardayCnt)\n" +
            ",sum(periodVehicleMileage)\n" +
            ",sum(pre_fuel_amount)\n" +
            ",sum(pre_oil_mileage)\n" +
            ",sum(pre_internal_fuel_amount)\n" +
            ",sum(pre_batch_cnt)\n" +
            ",sum(pre_subline_mileage)\n" +
            ",sum(preMaintenanceFee)\n" +
            ",sum(preProfit)\n" +
            ",sum(pre_internal_diesel)\n" +
            ",sum(pre_total_diesel)\n" +
            ",sum(preAccidentsCnt)\n" +
            ",sum(preRunTime)\n" +
            ",sum(preIdleDays)\n" +
            ",sum(preTotalCost)\n" +
            "from tb1\n" +
            ";\n";
}
