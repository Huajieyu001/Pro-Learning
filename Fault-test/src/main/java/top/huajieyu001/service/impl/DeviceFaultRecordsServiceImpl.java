package top.huajieyu001.service.impl;

import cn.hutool.Hutool;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.ArrayUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import top.huajieyu001.dto.FaultInfo;
import top.huajieyu001.dto.FaultResult;
import top.huajieyu001.entity.DeviceFaultRecords;
import top.huajieyu001.mapper.DeviceFaultRecordsMapper;
import top.huajieyu001.service.IDeviceFaultRecordsService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author Huajieyu
 * @since 2025-02-19
 */
@Service
public class DeviceFaultRecordsServiceImpl extends ServiceImpl<DeviceFaultRecordsMapper, DeviceFaultRecords> implements IDeviceFaultRecordsService {

    @Autowired
    private DeviceFaultRecordsMapper mapper;

    @Override
    public List<FaultResult> execute(String startTimeStr, String endTimeStr) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        LocalDateTime startTime = LocalDateTime.parse(startTimeStr, formatter);
        LocalDateTime endTime = LocalDateTime.parse(endTimeStr, formatter);

        List<FaultResult> resultList = new ArrayList<>();

        if (startTime.isBefore(endTime)) {
            List<DeviceFaultRecords> records = mapper.selectByStartTime2EndTime(startTimeStr, endTimeStr);
            long millis = System.currentTimeMillis();
            Instant instant = Instant.ofEpochMilli(millis);

            ZoneId zoneId = ZoneId.systemDefault();

            ZonedDateTime zonedDateTime = instant.atZone(zoneId);

            LocalDateTime searchTime = zonedDateTime.toLocalDateTime();

            Set<Long> deviceIdSet = new HashSet<>();

            records.forEach(e -> {
                deviceIdSet.add(e.getDeviceId());
            });

            List<Long> deviceIdList = new ArrayList<>(deviceIdSet);

            Collections.sort(deviceIdList);

            deviceIdList.forEach(e -> {
                List<DeviceFaultRecords> recordsByDeviceId = getRecordsByDeviceId(records, e);
                Set<String> faultIds = getFaultIds(recordsByDeviceId);
                List<FaultInfo> faultInfoList = new ArrayList<>();
                faultIds.forEach(v -> {
                    List<DeviceFaultRecords> recordsByFaultId = getRecordsByFaultId(records, v);
                    FaultInfo resultInfo = getResultInfo(recordsByFaultId, searchTime);
                    faultInfoList.add(resultInfo);
                });

                FaultResult result = getFaultResult(faultInfoList, e);
                resultList.add(result);
            });

            return resultList;
        }

        return null;
    }

    @Override
    public List<FaultResult> execute2(LocalDateTime startTime, LocalDateTime endTime) {

        if (startTime.isAfter(endTime)) {
            return Collections.emptyList();
        }

        List<DeviceFaultRecords> list = lambdaQuery()
                .between(DeviceFaultRecords::getOccurTime, startTime, endTime)
                .orderByAsc(DeviceFaultRecords::getDeviceId)
                .orderByAsc(DeviceFaultRecords::getFaultId)
                .orderByAsc(DeviceFaultRecords::getOpType)
                .list();

        long millis = System.currentTimeMillis();

        list.forEach(e -> e.setMillis(DateUtil.toInstant(e.getOccurTime()).toEpochMilli()));

        list.forEach(System.out::println);

        Map<String, List<DeviceFaultRecords>> collect = list.stream().collect(
                Collectors.groupingBy(s -> String.join(",", s.getDeviceId().toString(), s.getFaultId(), s.getFaultType().toString()))
        );

        List<FaultResult> resultList = new ArrayList<>();

        Map<Long, List<Map<String, Long>>> map = new HashMap<>();

        collect.keySet().forEach(key -> {
                    List<DeviceFaultRecords> recordsList = collect.get(key);
                    List<Map<String, Long>> mapList = map.get(recordsList.get(0).getDeviceId());

                    if (CollectionUtils.isEmpty(mapList)) {
                        mapList = new ArrayList<>();
                    }

                    Long retention = 0L;
                    Long fix = -1L;
                    Long fault = 0L;
                    // 每个List需要求出工单滞留时间，修复时间，故障时间
                    if (recordsList.size() == 1) {
                        // 工单滞留时间
                        retention = millis - recordsList.get(0).getMillis();
                        // 修复时间
                        fix = -1L;
                        // 故障时间
                        fault = retention;
                    } else if (recordsList.size() == 2) {
                        DeviceFaultRecords recordProduce = null;
                        DeviceFaultRecords recordAssign = null;

                        if (recordsList.get(0).getOpType() == 1) {
                            recordProduce = recordsList.get(0);
                            recordAssign = recordsList.get(1);
                        } else {
                            recordProduce = recordsList.get(1);
                            recordAssign = recordsList.get(0);
                        }

                        // 工单滞留时间
                        retention = recordAssign.getMillis() - recordProduce.getMillis();
                        // 修复时间
                        fix = -1L;
                        // 故障时间
                        fault = millis - recordProduce.getMillis();
                    } else if (recordsList.size() == 3) {
                        List<DeviceFaultRecords> sortedList = recordsList.stream().sorted((a, b) -> a.getOpType() > b.getOpType() ? 1 : 0).collect(Collectors.toList());

                        // 工单滞留时间
                        retention = sortedList.get(1).getMillis() - sortedList.get(0).getMillis();
                        // 修复时间
                        fix = sortedList.get(2).getMillis() - sortedList.get(1).getMillis();
                        // 故障时间
                        fault = sortedList.get(2).getMillis() - sortedList.get(0).getMillis();
                    }

                    Map<String, Long> longMap = new HashMap<>();
                    longMap.put("retention", retention);
                    longMap.put("fix", fix);
                    longMap.put("fault", fault);
                    longMap.put("type", recordsList.get(0).getFaultType().longValue());
                    mapList.add(longMap);
                    map.put(recordsList.get(0).getDeviceId(), mapList);
                }
        );

        map.forEach((k, v) -> {
            FaultResult result = new FaultResult();
            result.setDeviceId(k);
            List<Map<String, Long>> sfList = new ArrayList<>();
            List<Map<String, Long>> hdList = new ArrayList<>();
            v.forEach(e -> {
                if (e.get("type").equals(1L)) {
                    hdList.add(e);
                } else {
                    sfList.add(e);
                }
            });

            List<Long> hdRetentionList = hdList.stream().map(m -> m.get("retention")).collect(Collectors.toList());
            List<Long> hdFixList = hdList.stream().map(m -> m.get("fix")).collect(Collectors.toList());
            List<Long> hdFaultList = hdList.stream().map(m -> m.get("fault")).collect(Collectors.toList());

            List<Long> sfRetentionList = sfList.stream().map(m -> m.get("retention")).collect(Collectors.toList());
            List<Long> sfFixList = sfList.stream().map(m -> m.get("fix")).collect(Collectors.toList());
            List<Long> sfFaultList = sfList.stream().map(m -> m.get("fault")).collect(Collectors.toList());

            result.setHdAvgRetention((long) hdRetentionList.stream().mapToLong(Long::longValue).average().orElse(0.0));
            result.setHdMaxRetention(hdRetentionList.stream().mapToLong(Long::longValue).max().orElse(0L));
            result.setHdAvgFix((long) hdFixList.stream().filter(e -> e >= 0L).mapToLong(Long::longValue).average().orElse(0.0));
            result.setHdMaxFault(hdFaultList.stream().mapToLong(Long::longValue).max().orElse(0L));

            result.setSfAvgRetention((long) sfRetentionList.stream().mapToLong(Long::longValue).average().orElse(0.0));
            result.setSfMaxRetention(sfRetentionList.stream().mapToLong(Long::longValue).max().orElse(0L));
            result.setSfAvgFix((long) sfFixList.stream().filter(e -> e >= 0L).mapToLong(Long::longValue).average().orElse(0.0));
            result.setSfMaxFault(sfFaultList.stream().mapToLong(Long::longValue).max().orElse(0L));

            resultList.add(result);
        });
        resultList.sort((a, b) -> a.getDeviceId() > b.getDeviceId() ? 1 : -1);

        return resultList;
    }

    private List<DeviceFaultRecords> getRecordsByDeviceId(List<DeviceFaultRecords> records, Long id) {

        if (records == null || records.isEmpty()) {
            throw new RuntimeException("records is null! @top.huajieyu001.service.impl.DeviceFaultRecordsServiceImpl.getRecordsByDeviceId");
        }
        List<DeviceFaultRecords> resultList = new ArrayList<>();

        records.forEach(e -> {
            if (Objects.equals(id, e.getDeviceId())) {
                resultList.add(e);
            }
        });

        return resultList;
    }

    private Set<String> getFaultIds(List<DeviceFaultRecords> records) {
        Set<String> faultIdSet = new HashSet<>();

        records.forEach(e -> {
            faultIdSet.add(e.getFaultId());
        });

        return faultIdSet;
    }

    private List<DeviceFaultRecords> getRecordsByFaultId(List<DeviceFaultRecords> records, String faultId) {

        List<DeviceFaultRecords> recordsList = new ArrayList<>();
        records.sort((o1, o2) -> o1.getOpType() > o2.getOpType() ? 1 : 0);

        records.forEach(e -> {
            if (e.getFaultId().equals(faultId)) {
                recordsList.add(e);
            }
        });

        return recordsList;
    }

    private FaultInfo getResultInfo(List<DeviceFaultRecords> records, LocalDateTime searchTime) {
        if (records == null || records.isEmpty()) {
            return null;
        }

        records.sort((a, b) -> a.getOpType() > b.getOpType() ? 1 : 0);

        FaultInfo info = new FaultInfo(0L, 0L, 0L);
        if (records.size() == 1) {
            DeviceFaultRecords record = records.get(0);

            info.setRetention(countMillis(searchTime, record.getOccurTime()));
            info.setFix(0L);
            info.setFault(countMillis(searchTime, record.getOccurTime()));
            info.setSoftwareFault(record.getFaultType().equals(2));
        } else if (records.size() == 2) {
            DeviceFaultRecords record1 = records.get(0);
            DeviceFaultRecords record2 = records.get(1);

            info.setRetention(countMillis(record2.getOccurTime(), record1.getOccurTime()));
            info.setFix(0L);
            info.setFault(countMillis(searchTime, record1.getOccurTime()));
            info.setSoftwareFault(record1.getFaultType().equals(2));
        } else if (records.size() == 3) {
            DeviceFaultRecords record1 = records.get(0);
            DeviceFaultRecords record2 = records.get(1);
            DeviceFaultRecords record3 = records.get(2);

            info.setRetention(countMillis(record2.getOccurTime(), record1.getOccurTime()));
            info.setFix(countMillis(record3.getOccurTime(), record2.getOccurTime()));
            info.setFault(countMillis(record3.getOccurTime(), record1.getOccurTime()));
            info.setSoftwareFault(record1.getFaultType().equals(2));
        }

        return info;
    }

    private Long countMillis(LocalDateTime t1, LocalDateTime t2) {

        Duration duration = Duration.between(t2, t1);

        return duration.toMillis();
    }

    private FaultResult getFaultResult(List<FaultInfo> infoList, Long deviceId) {
        FaultResult result = new FaultResult(deviceId, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

        List<Long> sfFixList = new ArrayList<>();
        List<Long> hdFixList = new ArrayList<>();
        List<Long> sfRententionList = new ArrayList<>();
        List<Long> hdRententionList = new ArrayList<>();

        infoList.forEach(v -> {
            if (v.isSoftwareFault()) {
                if (v.getFault() > result.getSfMaxFault()) {
                    result.setSfMaxFault(v.getFault());
                }
                if (v.getRetention() > result.getSfMaxRetention()) {
                    result.setSfMaxRetention(v.getRetention());
                }
                if (v.getFix() > 0) {
                    sfFixList.add(v.getFix());
                }
                if (v.getRetention() > 0) {
                    sfRententionList.add(v.getRetention());
                }
            } else {
                if (v.getFault() > result.getHdMaxFault()) {
                    result.setHdMaxFault(v.getFault());
                }
                if (v.getRetention() > result.getHdMaxRetention()) {
                    result.setHdMaxRetention(v.getRetention());
                }
                if (v.getFix() > 0) {
                    hdFixList.add(v.getFix());
                }
                if (v.getRetention() > 0) {
                    hdRententionList.add(v.getRetention());
                }
            }
        });

        if (!sfFixList.isEmpty()) {
            result.setSfAvgFix(getAvg(sfFixList));
        }
        if (!sfRententionList.isEmpty()) {
            result.setSfAvgRetention(getAvg(sfRententionList));
        }
        if (!hdFixList.isEmpty()) {
            result.setHdAvgFix(getAvg(hdFixList));
        }
        if (!hdRententionList.isEmpty()) {
            result.setHdAvgRetention(getAvg(hdRententionList));
        }

        return result;
    }

    private Long getAvg(List<Long> numbers) {
        if (numbers == null || numbers.isEmpty()) {
            throw new RuntimeException();
        }

        int count = 0;
        Long total = 0L;
        for (Long number : numbers) {
            total += number;
            count++;
        }

        return total / count;
    }
}