package top.huajieyu001.controller;


import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import top.huajieyu001.dto.FaultResult;
import top.huajieyu001.service.IDeviceFaultRecordsService;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author Huajieyu
 * @since 2025-02-19
 */
@RestController
@RequestMapping("/device-fault-records")
@RequiredArgsConstructor
public class DeviceFaultRecordsController {

    private final IDeviceFaultRecordsService iDeviceFaultRecordsService;

    @PostMapping("/test")
    public List<FaultResult> execute(@RequestParam("startTime") String startTimeStr, @RequestParam("endTime") String endTimeStr) {
        return iDeviceFaultRecordsService.execute(startTimeStr, endTimeStr);
    }

    @PostMapping("/test2")
    public List<FaultResult> execute2(@RequestParam("startTime")@DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime startTime,
                                      @RequestParam("endTime")@DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime endTime)
    {
        return iDeviceFaultRecordsService.execute2(startTime, endTime);
    }
}
